require "httpclient"

module Embulk
  module Input
    module Zendesk
      class Client
        attr_reader :config

        def initialize(config, retryer)
          @config = config
          @retryer = retryer
        end

        def client
          client = HTTPClient.new
          # client.debug_dev = STDOUT
          return set_auth(client)
        end

        def set_auth(client)
          # https://developer.zendesk.com/rest_api/docs/core/introduction#security-and-authentication
          case config[:auth_method]
          when "basic"
            client.set_auth(config[:login_url], config[:username], config[:password])
          when "token"
            client.set_auth(config[:login_url], "#{config[:username]}/token", config[:token])
          when "oauth"
            client.default_header = {
              "Authorization" => "Bearer #{config[:access_token]}"
            }
          end
          client
        end

        def request(path, query = {})
          u = URI.parse(config[:login_url])
          u.path = path

          @retryer.with_retry do
            response = client.get(u.to_s, query)

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
              retry_after = response.headers["Retry-After"].to_i
              Embulk.logger.warn "Rate Limited. Waiting #{retry_after} seconds to retry"
              sleep retry_after
              throw :retry
            when 500, 503
              # 503 possible rate limit
              retry_after = response.headers["Retry-After"].to_i
              if retry_after
                sleep retry_after
                throw :retry
              else
                raise "[#{status_code}] temporally failure."
              end
            else
              raise "Server returns unknown status code (#{status_code})"
            end
          end
        end

        def tickets(start_time = 0, known_ids = [], &block)
          endpoint = "/api/v2/incremental/tickets"
          response = request(endpoint, start_time: start_time)
          begin
            data = JSON.parse(response.body)
          rescue => e
            raise Embulk::DataError.new(e)
          end

          Embulk.logger.debug "start_time:#{start_time} (#{Time.at(start_time)}) count:#{data["count"]} next_page:#{data["next_page"]} end_time:#{data["end_time"]} "
          data["tickets"].each do |ticket|
            # de-duplicated tickets.
            # https://developer.zendesk.com/rest_api/docs/core/incremental_export#usage-notes
            # https://github.com/zendesk/zendesk_api_client_rb/issues/251
            next if known_ids.include?(ticket["id"])

            known_ids << ticket["id"]
            block.call ticket
          end

          # NOTE: If count is less than 1000, then stop paginating.
          #       Otherwise, use the next_page URL to get the next page of results.
          #       https://developer.zendesk.com/rest_api/docs/core/incremental_export#pagination
          if data["count"] == 1000
            tickets(data["end_time"], known_ids, &block)
          end
        end
      end
    end
  end
end
