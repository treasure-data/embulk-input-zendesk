require "strscan"
require "httpclient"
require 'thread/pool'

module Embulk
  module Input
    module Zendesk
      class Client
        attr_reader :config

        PARTIAL_RECORDS_SIZE = 50
        PARTIAL_RECORDS_BYTE_SIZE = 50000
        THREADPOOL_MIN_SIZE = 50
        THREADPOOL_MAX_SIZE = 100
        AVAILABLE_INCREMENTAL_EXPORT = %w(tickets users organizations ticket_events).freeze
        UNAVAILABLE_INCREMENTAL_EXPORT = %w(ticket_fields ticket_forms ticket_metrics).freeze
        AVAILABLE_TARGETS = AVAILABLE_INCREMENTAL_EXPORT + UNAVAILABLE_INCREMENTAL_EXPORT

        def initialize(config)
          @config = config
        end

        def httpclient
          # multi-threading + retry can create lot of instances, and each will keep connecting
          # re-using instance in multi threads can help to omit cleanup code
          @httpclient ||=
            begin
              clnt = HTTPClient.new
              clnt.connect_timeout = 240 # default:60 is not enough for huge data
              clnt.receive_timeout = 240 # better change default receive_timeout too
              # httpclient.debug_dev = STDOUT
              set_auth(clnt)
            end
        end

        def pool
          @pool ||= Thread.pool(THREADPOOL_MIN_SIZE, THREADPOOL_MAX_SIZE)
        end

        def validate_config
          validate_credentials
          validate_target
        end

        def validate_credentials
          valid = case config[:auth_method]
          when "basic"
            config[:username] && config[:password]
          when "token"
            config[:username] && config[:token]
          when "oauth"
            config[:access_token]
          else
            raise Embulk::ConfigError.new("Unknown auth_method (#{config[:auth_method]}). Should pick one from 'basic', 'token' or 'oauth'.")
          end

          unless valid
            raise Embulk::ConfigError.new("Missing required credentials for #{config[:auth_method]}")
          end
        end

        def validate_target
          unless AVAILABLE_TARGETS.include?(config[:target])
            raise Embulk::ConfigError.new("target: '#{config[:target]}' is not supported. Supported targets are #{AVAILABLE_TARGETS.join(", ")}.")
          end
        end

        # they have both Incremental API and non-incremental API
        %w(tickets users organizations).each do |target|
          define_method(target) do |partial = true, start_time = 0, &block|
            # Always use incremental_export. There is some difference between incremental_export and export.
            incremental_export("/api/v2/incremental/#{target}.json", target, start_time, [], partial, &block)
          end
        end

        # they have incremental API only
        %w(ticket_events).each do |target|
          define_method(target) do |partial = true, start_time = 0, &block|
            path = "/api/v2/incremental/#{target}"
            incremental_export(path, target, start_time, [], partial, &block)
          end
        end

        # they have non-incremental API only
        UNAVAILABLE_INCREMENTAL_EXPORT.each do |target|
          define_method(target) do |partial = true, start_time = 0, &block|
            path = "/api/v2/#{target}.json"
            if partial
              export(path, target, &block)
            else
              export_parallel(path, target, start_time, &block)
            end
          end
        end

        def fetch_subresource(record_id, base, target)
          rename_jruby_thread(Thread.current)
          Embulk.logger.info "Fetching subresource #{target} of #{base}:#{record_id}"
          response = request("/api/v2/#{base}/#{record_id}/#{target}.json")
          return [] if response.status == 404

          begin
            data = JSON.parse(response.body)
            data[target]
          rescue => e
            raise Embulk::DataError.new(e)
          end
        end

        private

        def export_parallel(path, key, start_time = 0, &block)
          per_page = 100 # 100 is maximum https://developer.zendesk.com/rest_api/docs/core/introduction#pagination
          first_response = request(path, per_page: per_page, page: 1)
          first_fetched = JSON.parse(first_response.body)
          total_count = first_fetched["count"]
          last_page_num = (total_count / per_page.to_f).ceil
          Embulk.logger.info "#{key} records=#{total_count} last_page=#{last_page_num}"

          known_ticket_ids = []
          first_fetched[key].uniq { |r| r['id'] }.each do |record|
            block.call record
            # known_ticket_ids: collect fetched ticket IDs, to exclude in next step
            known_ticket_ids << record['ticket_id'] if key == 'ticket_metrics'
          end

          lock = Mutex.new
          (2..last_page_num).each do |page|
            pool.process do
              rename_jruby_thread(Thread.current)
              response = request(path, per_page: per_page, page: page)
              fetched_records = extract_records_from_response(response, key)
              Embulk.logger.info "Fetched #{key} on page=#{page} >>> size: #{fetched_records.length}"
              fetched_records.uniq { |r| r['id'] }.each do |record|
                block.call record
                # known_ticket_ids: collect fetched ticket IDs, to exclude in next step
                lock.synchronize { known_ticket_ids << record['ticket_id'] if key == 'ticket_metrics' }
              end
            end
          end

          pool.wait(:done)

          # If target is 'ticket_metrics'
          # need to double check with list of all tickets and pull missing ones
          # Not explicitly stated in documents, but here is Zendesk support's response:
          # > The ticket_metrics.json endpoint (list) will return live tickets. What you are experiencing is the cutoff of archived tickets.
          # > I recommend the usage of the Incremental Exports api endpoint for retrieving "All" tickets from the beginning of time.
          # > The ticket metrics api would be possibly better used by supplying a ticket ID to get the metrics of the particular ticket you wish to get get metrics on.
          if key == 'ticket_metrics'
            incremental_export('/api/v2/incremental/tickets.json', 'tickets', start_time, known_ticket_ids, false) do |ticket|
              response = request("/api/v2/tickets/#{ticket['id']}/metrics.json")
              metrics = JSON.parse(response.body)
              block.call metrics['ticket_metric'] if metrics['ticket_metric']
            end
          end
          pool.shutdown
          nil # this is necessary different with incremental_export
        end

        def export(path, key, page = 1, &block)
          per_page = PARTIAL_RECORDS_SIZE
          Embulk.logger.info("Fetching #{path} with page=#{page} (partial)")

          response = request(path, per_page: per_page, page: page)

          begin
            data = JSON.parse(response.body)
          rescue => e
            raise Embulk::DataError.new(e)
          end

          data[key].each do |record|
            block.call record
          end
        end

        def incremental_export(path, key, start_time = 0, known_ids = [], partial = true, &block)
          if partial
            records = request_partial(path, {start_time: start_time}).first(5)
            records.uniq{|r| r["id"]}.each do |record|
              block.call record
            end
            return
          end

          last_data = loop do
            start_fetching = Time.now
            response = request(path, {start_time: start_time})
            begin
              data = JSON.parse(response.body)
            rescue => e
              raise Embulk::DataError.new(e)
            end
            actual_fetched = 0
            records = data[key]
            records.each do |record|
              # https://developer.zendesk.com/rest_api/docs/core/incremental_export#excluding-system-updates
              # "generated_timestamp" will be updated when Zendesk internal changing
              # "updated_at" will be updated when ticket data was changed
              # start_time for query parameter will be processed on Zendesk with generated_timestamp,
              # but it was calculated by record' updated_at time.
              # So the doesn't changed record from previous import would be appear by Zendesk internal changes.
              # We ignore record that has updated_at <= start_time
              if start_time && record["generated_timestamp"] && record["updated_at"]
                updated_at = Time.parse(record["updated_at"])
                next if updated_at <= Time.at(start_time)
              end

              # de-duplicated records.
              # https://developer.zendesk.com/rest_api/docs/core/incremental_export#usage-notes
              # https://github.com/zendesk/zendesk_api_client_rb/issues/251
              next if known_ids.include?(record["id"])

              known_ids << record["id"]
              pool.process {
                rename_jruby_thread(Thread.current)
                yield(record)
              }
              actual_fetched += 1
            end
            Embulk.logger.info "Fetched #{actual_fetched} records from start_time:#{start_time} (#{Time.at(start_time)}) within #{Time.now.to_i - start_fetching.to_i} seconds"
            start_time = data["end_time"]

            # NOTE: If count is less than 1000, then stop paginating.
            #       Otherwise, use the next_page URL to get the next page of results.
            #       https://developer.zendesk.com/rest_api/docs/core/incremental_export#pagination
            break data if data["count"] < 1000
          end

          pool.shutdown
          last_data
        end

        def extract_records_from_response(response, key)
          begin
            data = JSON.parse(response.body)
            data[key]
          rescue => e
            raise Embulk::DataError.new(e)
          end
        end

        def retryer
          PerfectRetry.new do |config|
            config.limit = @config[:retry_limit]
            config.logger = Embulk.logger
            config.log_level = nil
            config.dont_rescues = [Embulk::DataError, Embulk::ConfigError]
            config.sleep = lambda{|n| @config[:retry_initial_wait_sec]* (2 ** (n-1)) }
            config.raise_original_error = true
          end
        end

        def set_auth(httpclient)
          validate_credentials

          # https://developer.zendesk.com/rest_api/docs/core/introduction#security-and-authentication
          case config[:auth_method]
          when "basic"
            httpclient.set_auth(config[:login_url], config[:username], config[:password])
          when "token"
            httpclient.set_auth(config[:login_url], "#{config[:username]}/token", config[:token])
          when "oauth"
            httpclient.default_header = {
              "Authorization" => "Bearer #{config[:access_token]}"
            }
          end
          httpclient
        end

        def request(path, query = {})
          u = URI.parse(config[:login_url])
          u.path = path

          retryer.with_retry do
            Embulk.logger.debug "Fetching #{u.to_s}"
            response = httpclient.get(u.to_s, query, follow_redirect: true)

            handle_response(response.status, response.headers, response.body)
            response
          end
        end

        def request_partial(path, query = {})
          # NOTE: This is a dirty hack for quick response using incremental_export API.
          #       Disconnect socket when received PARTIAL_RECORDS_BYTE_SIZE bytes,
          #       And extract valid JSONs from received bytes (extract_valid_json_from_chunk method)
          u = URI.parse(config[:login_url])
          u.path = path

          retryer.with_retry do
            Embulk.logger.debug "Fetching #{u.to_s}"
            buf = ""
            auth_retry = 0
            httpclient.get(u.to_s, query, follow_redirect: true) do |message, chunk|
              if message.status == 401
                # First request will fail by 401 because not included credentials.
                # HTTPClient will retry request with credentials.
                if auth_retry.zero?
                  auth_retry += 1
                  next
                end
              end
              handle_response(message.status, message.headers, chunk)

              buf << chunk
              break if buf.bytesize > PARTIAL_RECORDS_BYTE_SIZE
            end
            extract_valid_json_from_chunk(buf).map do |json|
              JSON.parse(json)
            end
          end
        end

        def extract_valid_json_from_chunk(chunk)
          # Drip JSON objects from incomplete string
          #
          # e.g.:
          # chunk = '{"ticket_events":[{"foo":1},{"foo":2},{"fo'
          # extract_valid_json_from_chunk(chunk) #=>  ['{"foo":1}', '{"foo":2}']
          result = []

          # omit '{"tickets":[' prefix. See test/fixtures/tickets.json for actual response.
          s = StringScanner.new(chunk.scrub.gsub(%r!^{".*?":\[!,""))
          while !s.eos?
            opener = s.scan(/{/)
            break unless opener
            buf = opener # Initialize `buf` as "{"
            while content = s.scan(/.*?}/) # grab data from start to next "}"
              buf << content
              if (JSON.parse(buf) rescue false) # if JSON.parse success, `buf` is valid JSON. we'll take it.
                result << buf.dup
                break
              end
            end
            s.scan(/[^{]*/) # skip until next "{". `chunk` has comma separeted objects like  '},{'. skip that comma.
          end
          result
        end

        def wait_rate_limit(retry_after)
          Embulk.logger.warn "Rate Limited. Waiting #{retry_after} seconds to retry"
          sleep retry_after
          throw :retry
        end

        def handle_response(status_code, headers, body)
          # https://developer.zendesk.com/rest_api/docs/core/introduction#response-format
          case status_code
          when 200, 404
            # 404 would be returned e.g. ticket comments are empty (on fetch_subresource method)
          when 409
            raise "[#{status_code}] temporally failure."
          when 422
            begin
              payload = JSON.parse(body)
              if payload["description"].start_with?("Too recent start_time.")
                # That means "No records from start_time". We can recognize it same as 200.
                return
              end
            rescue
              # Failed to parse response.body as JSON
              raise Embulk::ConfigError.new("[#{status_code}] #{body}")
            end

            # 422 and it isn't "Too recent start_time"
            raise Embulk::ConfigError.new("[#{status_code}] #{body}")
          when 429
            # rate limit
            retry_after = headers["Retry-After"]
            wait_rate_limit(retry_after.to_i)
          when 400..500
            # Won't retry for 4xx range errors except above. Almost they should be ConfigError e.g. 403 Forbidden
            raise Embulk::ConfigError.new("[#{status_code}] #{body}")
          when 500, 503
            # 503 is possible rate limit
            retry_after = headers["Retry-After"]
            if retry_after
              wait_rate_limit(retry_after.to_i)
            else
              raise "[#{status_code}] temporally failure."
            end
          else
            raise "Server returns unknown status code (#{status_code}) #{body}"
          end
        end

        # JRuby named spawn thread with runtime absolute path, this method is to avoid that info in logs
        def rename_jruby_thread(thread)
          return unless defined?(JRuby)
          thread_r = JRuby.reference(thread)
          native_name = thread_r.native_thread.name
          # https://github.com/jruby/jruby/blob/9.0.4.0/core/src/main/java/org/jruby/RubyThread.java#L563
          path_index = native_name.index(':') || 0
          thread_r.native_thread.name = native_name[0..path_index-1]
        end
      end
    end
  end
end
