require "strscan"
require "httpclient"
require 'concurrent'

module Embulk
  module Input
    module Zendesk
      class Client
        attr_reader :config

        PARTIAL_RECORDS_SIZE = 50
        PARTIAL_RECORDS_BYTE_SIZE = 50000
        AVAILABLE_INCREMENTAL_EXPORT = %w(tickets users organizations ticket_events ticket_metrics).freeze
        UNAVAILABLE_INCREMENTAL_EXPORT = %w(ticket_fields ticket_forms).freeze
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

        def create_pool
          Concurrent::ThreadPoolExecutor.new(
            min_threads: 10,
            max_threads: 100,
            max_queue: 10_000,
            fallback_policy: :caller_runs
          )
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
        # 170717: `ticket_events` can use standard endpoint format now, ie. `<target>.json`
        %w(tickets ticket_events users organizations).each do |target|
          define_method(target) do |partial = true, start_time = 0, &block|
            # Always use incremental_export. There is some difference between incremental_export and export.
            incremental_export("/api/v2/incremental/#{target}.json", target, start_time, [], partial, &block)
          end
        end

        # Ticket metrics will need to be export using both the non incremental and incremental on ticket
        # We provide support by filter out ticket_metrics with created at smaller than start time
        # while passing the incremental start time to the incremental ticket/ticket_metrics export
        define_method('ticket_metrics') do |partial = true, start_time = 0, &block|
          if partial
            # If partial export then we need to use the old end point. Since new end point return both ticket and
            # ticket metric with ticket come first so the current approach that cut off the response packet won't work
            # Since partial is only use for preview and guess so this should be fine
            export('/api/v2/ticket_metrics.json', 'ticket_metrics', &block)
          else
            incremental_export('/api/v2/incremental/tickets.json', 'metric_sets', start_time, [], partial, { include: 'metric_sets' }, &block)
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

          first_fetched[key].uniq { |r| r['id'] }.each do |record|
            block.call record
          end

          execute_thread_pool do |pool|
            (2..last_page_num).each do |page|
              pool.post do
                response = request(path, per_page: per_page, page: page)
                fetched_records = extract_records_from_response(response, key)
                Embulk.logger.info "Fetched #{key} on page=#{page} >>> size: #{fetched_records.length}"
                fetched_records.uniq { |r| r['id'] }.each do |record|
                  block.call record
                end
              end
            end
          end

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

        def incremental_export(path, key, start_time = 0, known_ids = [], partial = true, query = {}, &block)
          query.merge!(start_time: start_time)
          if partial
            records = request_partial(path, query).first(5)
            records.uniq{|r| r["id"]}.each do |record|
              block.call record
            end
            return
          end

          execute_thread_pool do |pool|
            loop do
              start_fetching = Time.now
              response = request(path, query)
              actual_fetched = 0
              data = JSON.parse(response.body)
              # no key found in response occasionally => retry
              raise TempError, "No '#{key}' found in JSON response" unless data.key? key
              data[key].each do |record|
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
                pool.post { block.call record }
                actual_fetched += 1
              end
              Embulk.logger.info "Fetched #{actual_fetched} records from start_time:#{start_time} (#{Time.at(start_time)}) within #{Time.now.to_i - start_fetching.to_i} seconds"
              start_time = data["end_time"]

              # NOTE: If count is less than 1000, then stop paginating.
              #       Otherwise, use the next_page URL to get the next page of results.
              #       https://developer.zendesk.com/rest_api/docs/core/incremental_export#pagination
              break data if data["count"] < 1000
            end
          end
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

        def execute_thread_pool(&block)
          pool = create_pool
          pr = PerfectRetry.new do |config|
            config.limit = @config[:retry_limit]
            config.logger = Embulk.logger
            config.log_level = nil
            config.rescues = [TempError]
            config.sleep = lambda{|n| @config[:retry_initial_wait_sec]* (2 ** (n-1)) }
          end
          pr.with_retry { block.call(pool) }
        rescue => e
          raise Embulk::DataError.new(e)
        ensure
          Embulk.logger.info 'ThreadPool shutting down...'
          pool.shutdown
          pool.wait_for_termination
          Embulk.logger.info "ThreadPool shutdown? #{pool.shutdown?}"
        end
      end

      class TempError < StandardError
      end
    end
  end
end
