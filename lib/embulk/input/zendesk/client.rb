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
            case response.status
            when 200
              response
            when 429
              # rate limit
              retry_after = response.headers["Retry-After"].to_i
              Embulk.logger.warn "Rate Limited. Waiting #{retry_after} seconds to retry"
              sleep retry_after
              throw :retry
            when 500
              raise "Server returns 500"
            else # TODO: investigate all possible response
              raise "Server returns unknown status code (#{response.status})"
            end
          end
        end

        def tickets(start_time = 0, &block)
          endpoint = "/api/v2/incremental/tickets"
          response = request(endpoint, start_time: start_time)
          begin
            data = JSON.parse(response.body)
          rescue => e
            raise Embulk::DataError.new(e)
          end

          data["tickets"].each do |ticket|
            block.call ticket
          end

          if data["count"] == 1000
            tickets(data["end_time"], &block)
          end
        end
      end
    end
  end
end
