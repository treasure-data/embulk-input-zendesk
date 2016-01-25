require "httpclient"

module Embulk
  module Input
    module Zendesk
      class Client
        attr_reader :config

        PARTIAL_RECORDS_SIZE = 50
        AVAILABLE_TARGETS = %w(
          tickets ticket_events users organizations
        ).freeze

        def initialize(config)
          @config = config
        end

        def httpclient
          httpclient = HTTPClient.new
          # httpclient.debug_dev = STDOUT
          return set_auth(httpclient)
        end

        def validate_config
          validate_credentials && validate_target
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
            raise Embulk::ConfigError.new("target: #{config[:target]} is not supported.")
          end
        end

        %w(tickets users organizations).each do |target|
          define_method(target) do |partial = true, start_time = 0, &block|
            if partial
              export("/api/v2/#{target}.json", target, PARTIAL_RECORDS_SIZE, &block)
            else
              incremental_export("/api/v2/incremental/#{target}.json", target, start_time, [], &block)
            end
          end
        end

        def ticket_events(partial = true, start_time = 0, &block)
          # NOTE: ticket_events only have full export API
          path = "/api/v2/incremental/ticket_events"
          incremental_export(path, "ticket_events", start_time, [], &block)
        end

        private

        def export(path, key, per_page, &block)
          # for `embulk guess` and `embulk preview` to fetch ~50 tickets only.
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
        end

        def incremental_export(path, key, start_time = 0, known_ids = [], &block)
          # for `embulk run` to fetch all tickets.
          response = request(path, start_time: start_time)

          begin
            data = JSON.parse(response.body)
          rescue => e
            raise Embulk::DataError.new(e)
          end

          Embulk.logger.debug "start_time:#{start_time} (#{Time.at(start_time)}) count:#{data["count"]} next_page:#{data["next_page"]} end_time:#{data["end_time"]} "
          data[key].each do |record|
            # de-duplicated tickets.
            # https://developer.zendesk.com/rest_api/docs/core/incremental_export#usage-notes
            # https://github.com/zendesk/zendesk_api_client_rb/issues/251
            next if known_ids.include?(record["id"])

            known_ids << record["id"]
            block.call record
          end

          # NOTE: If count is less than 1000, then stop paginating.
          #       Otherwise, use the next_page URL to get the next page of results.
          #       https://developer.zendesk.com/rest_api/docs/core/incremental_export#pagination
          if data["count"] == 1000
            incremental_export(path, key, data["end_time"], known_ids, &block)
          end
        end

        def retryer
          PerfectRetry.new do |config|
            config.limit = @config[:retry_limit]
            config.logger = Embulk.logger
            config.log_level = nil
            config.dont_rescues = [Embulk::DataError, Embulk::ConfigError]
            config.sleep = lambda{|n| @config[:retry_initial_wait_sec]* (2 ** (n-1)) }
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
            response = httpclient.get(u.to_s, query, follow_redirect: true)

            # https://developer.zendesk.com/rest_api/docs/core/introduction#response-format
            status_code = response.status
            case status_code
            when 200
              response
            when 400, 401
              raise Embulk::ConfigError.new("[#{status_code}] #{response.body}")
            when 409
              raise "[#{status_code}] temporally failure."
            when 429
              # rate limit
              retry_after = response.headers["Retry-After"]
              wait_rate_limit(retry_after.to_i)
            when 500, 503
              # 503 is possible rate limit
              retry_after = response.headers["Retry-After"]
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

        def wait_rate_limit(retry_after)
          Embulk.logger.warn "Rate Limited. Waiting #{retry_after} seconds to retry"
          sleep retry_after
          throw :retry
        end

      end
    end
  end
end
