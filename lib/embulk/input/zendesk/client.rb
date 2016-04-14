require "strscan"
require "httpclient"

module Embulk
  module Input
    module Zendesk
      class Client
        attr_reader :config

        PARTIAL_RECORDS_SIZE = 50
        PARTIAL_RECORDS_BYTE_SIZE = 50000
        AVAILABLE_INCREMENTAL_EXPORT = %w(tickets users organizations ticket_events).freeze
        UNAVAILABLE_INCREMENTAL_EXPORT = %w(ticket_fields ticket_forms ticket_metrics).freeze
        AVAILABLE_TARGETS = AVAILABLE_INCREMENTAL_EXPORT + UNAVAILABLE_INCREMENTAL_EXPORT

        def initialize(config)
          @config = config
        end

        def httpclient
          httpclient = HTTPClient.new
          # httpclient.debug_dev = STDOUT
          return set_auth(httpclient)
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
            export(path, target, partial ? PARTIAL_RECORDS_SIZE : 1000, &block)
          end
        end

        def fetch_subresource(record_id, base, target)
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

        def export(path, key, per_page, &block)
          # for `embulk guess` and `embulk preview` to fetch ~50 records only.
          # incremental export API has supported only 1000 per page, it is too large to guess/preview
          Embulk.logger.debug "#{path} with per_page: #{per_page}"
          response = request(path, per_page: per_page)

          begin
            data = JSON.parse(response.body)
          rescue => e
            raise Embulk::DataError.new(e)
          end

          data[key].each do |record|
            block.call record
          end

          nil # this is necessary different with incremental_export
        end

        def incremental_export(path, key, start_time = 0, known_ids = [], partial = true, &block)
          if partial
            records = request_partial(path, {start_time: start_time}).first(5)
          else
            response = request(path, {start_time: start_time})
            begin
              data = JSON.parse(response.body)
            rescue => e
              raise Embulk::DataError.new(e)
            end
            Embulk.logger.debug "start_time:#{start_time} (#{Time.at(start_time)}) count:#{data["count"]} next_page:#{data["next_page"]} end_time:#{data["end_time"]} "
            records = data[key]
          end

          records.each do |record|
            # de-duplicated records.
            # https://developer.zendesk.com/rest_api/docs/core/incremental_export#usage-notes
            # https://github.com/zendesk/zendesk_api_client_rb/issues/251
            next if known_ids.include?(record["id"])

            known_ids << record["id"]
            block.call record
          end
          return if partial

          # NOTE: If count is less than 1000, then stop paginating.
          #       Otherwise, use the next_page URL to get the next page of results.
          #       https://developer.zendesk.com/rest_api/docs/core/incremental_export#pagination
          if data["count"] == 1000
            incremental_export(path, key, data["end_time"], known_ids, partial, &block)
          else
            data
          end
        end

        def retryer
          PerfectRetry.new do |config|
            config.limit = @config[:retry_limit]
            config.logger = Embulk.logger
            config.log_level = nil
            config.dont_rescues = [Embulk::DataError, Embulk::ConfigError]
            config.sleep = lambda{|n| @config[:retry_initial_wait_sec]* (2 ** (n-1)) }
            config.prefer_original_backtrace = true
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
            raise "Server returns unknown status code (#{status_code})"
          end
        end
      end
    end
  end
end
