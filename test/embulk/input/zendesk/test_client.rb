require "embulk"
Embulk.setup

require "yaml"
require "embulk/input/zendesk"
require "override_assert_raise"
require "fixture_helper"
require "capture_io"

module Embulk
  module Input
    module Zendesk
      class TestClient < Test::Unit::TestCase
        include OverrideAssertRaise
        include FixtureHelper
        include CaptureIo

        sub_test_case "tickets" do
          sub_test_case "partial" do
            def client
              @client ||= Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token, retry_limit: 1, retry_initial_wait_sec: 0)
            end

            setup do
              stub(Embulk).logger { Logger.new(File::NULL) }
              @httpclient = client.httpclient
              stub(client).httpclient { @httpclient }
            end

            test "fetch tickets" do
              tickets = [
                {"id" => 1},
                {"id" => 2},
              ]
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: tickets
                }.to_json
              ].join("\r\n")

              handler = proc { }
              tickets.each do |ticket|
                mock(handler).call(ticket)
              end
              client.tickets(&handler)
            end

            test "raise DataError when invalid JSON response" do
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                "[[[" # invalid json
              ].join("\r\n")

              assert_raise(DataError) do
                client.tickets
              end
            end
          end

          sub_test_case "all" do
            def client
              @client ||= Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token, retry_limit: 1, retry_initial_wait_sec: 0)
            end

            setup do
              stub(Embulk).logger { Logger.new(File::NULL) }
              @httpclient = client.httpclient
              stub(client).httpclient { @httpclient }
            end

            test "fetch tickets" do
              tickets = [
                {"id" => 1},
                {"id" => 2},
              ]
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: tickets
                }.to_json
              ].join("\r\n")

              handler = proc { }
              tickets.each do |ticket|
                mock(handler).call(ticket)
              end
              client.tickets(false, &handler)
            end

            test "fetch tickets without duplicated" do
              tickets = [
                {"id" => 1},
                {"id" => 2},
                {"id" => 1},
                {"id" => 1},
              ]
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: tickets
                }.to_json
              ].join("\r\n")

              handler = proc { }
              mock(handler).call(anything).twice
              client.tickets(false, &handler)
            end

            test "fetch tickets with next_page" do
              end_time = Time.now.to_i

              response_1 = [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: [{"id" => 1}],
                  count: 1000,
                  end_time: end_time,
                }.to_json
              ].join("\r\n")

              response_2 = [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: [{"id" => 2}],
                  count: 2,
                }.to_json
              ].join("\r\n")

              @httpclient.test_loopback_http_response << response_1
              @httpclient.test_loopback_http_response << response_2

              handler = proc { }
              mock(handler).call(anything).twice
              client.tickets(false, &handler)
            end

            test "raise DataError when invalid JSON response" do
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                "[[[" # invalid json
              ].join("\r\n")

              assert_raise(DataError) do
                client.tickets(false)
              end
            end
          end
        end

        sub_test_case "targets" do
          def client
            @client ||= Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token, retry_limit: 1, retry_initial_wait_sec: 0)
          end

          setup do
            stub(Embulk).logger { Logger.new(File::NULL) }
            @httpclient = client.httpclient
            stub(client).httpclient { @httpclient }
          end

          sub_test_case "ticket_events" do
            test "invoke incremental_export when partial=true" do
              mock(client).incremental_export(anything, "ticket_events", anything, [])
              client.ticket_events(true)
            end

            test "invoke incremental_export when partial=false" do
              mock(client).incremental_export(anything, "ticket_events", anything, [])
              client.ticket_events(false)
            end
          end
        end


        sub_test_case "auth" do
          test "httpclient call validate_credentials" do
            client = Client.new({})
            mock(client).validate_credentials
            client.httpclient
          end

          sub_test_case "auth_method: basic" do
            test "don't raise on validate when username and password given" do
              client = Client.new(login_url: login_url, auth_method: "basic", username: username, password: password)
              assert_nothing_raised do
                client.validate_credentials
              end

              any_instance_of(HTTPClient) do |klass|
                mock(klass).set_auth(login_url, username, password)
              end
              client.httpclient
            end

            test "set_auth called with valid credential" do
              client = Client.new(login_url: login_url, auth_method: "basic", username: username, password: password)

              any_instance_of(HTTPClient) do |klass|
                mock(klass).set_auth(login_url, username, password)
              end
              client.httpclient
            end

            data do
              [
                ["username", {username: "foo@example.com"}],
                ["password", {password: "passWORD"}],
                ["nothing both", {}],
              ]
            end
            test "username only given" do |config|
              client = Client.new(config.merge(auth_method: "basic"))
              assert_raise(ConfigError) do
                client.validate_credentials
              end
            end
          end

          sub_test_case "auth_method: token" do
            test "don't raise on validate when username and token given" do
              client = Client.new(login_url: login_url, auth_method: "token", username: username, token: token)
              assert_nothing_raised do
                client.validate_credentials
              end
            end

            test "set_auth called with valid credential" do
              client = Client.new(login_url: login_url, auth_method: "token", username: username, token: token)

              any_instance_of(HTTPClient) do |klass|
                mock(klass).set_auth(login_url, "#{username}/token", token)
              end
              client.httpclient
            end

            data do
              [
                ["username", {username: "foo@example.com"}],
                ["token", {token: "TOKEN"}],
                ["nothing both", {}],
              ]
            end
            test "username only given" do |config|
              client = Client.new(config.merge(auth_method: "token"))
              assert_raise(ConfigError) do
                client.validate_credentials
              end
            end
          end

          sub_test_case "auth_method: oauth" do
            test "don't raise on validate when access_token given" do
              client = Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token)
              assert_nothing_raised do
                client.validate_credentials
              end
            end

            test "set default header with valid credential" do
              client = Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token)

              any_instance_of(HTTPClient) do |klass|
                mock(klass).default_header = {
                  "Authorization" => "Bearer #{access_token}"
                }
              end
              client.httpclient
            end

            test "access_token not given" do |config|
              client = Client.new(auth_method: "oauth")
              assert_raise(ConfigError) do
                client.validate_credentials
              end
            end
          end

          sub_test_case "auth_method: unknown" do
            test "raise on validate" do
              client = Client.new(auth_method: "unknown")
              assert_raise(ConfigError) do
                client.validate_credentials
              end
            end
          end
        end

        sub_test_case "retry" do
          def client
            @client ||= Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token, retry_limit: 2, retry_initial_wait_sec: 0)
          end

          def stub_response(status, headers = [])
            @httpclient.test_loopback_http_response << [
              "HTTP/1.1 #{status}",
              "Content-Type: application/json",
              headers.join("\r\n"),
              "",
              {
                tickets: []
              }.to_json
            ].join("\r\n")
          end

          setup do
            retryer = PerfectRetry.new do |conf|
              conf.dont_rescues = [Exception] # Don't care any exceptions to retry
            end

            stub(Embulk).logger { Logger.new(File::NULL) }
            @httpclient = client.httpclient
            stub(client).httpclient { @httpclient }
            stub(client).retryer { retryer }
            PerfectRetry.disable!
          end

          teardown do
            PerfectRetry.enable!
          end

          test "400" do
            stub_response(400)
            assert_raise(ConfigError) do
              client.tickets(&proc{})
            end
          end

          test "401" do
            stub_response(401)
            assert_raise(ConfigError) do
              client.tickets(&proc{})
            end
          end

          test "409" do
            stub_response(409)
            assert_raise(StandardError) do
              client.tickets(&proc{})
            end
          end

          test "429" do
            after = "123"
            stub_response(429, ["Retry-After: #{after}"])
            mock(client).sleep after.to_i
            assert_throw(:retry) do
              client.tickets(&proc{})
            end
          end

          test "500" do
            stub_response(500)
            assert_raise(StandardError) do
              client.tickets(&proc{})
            end
          end

          test "503" do
            stub_response(503)
            assert_raise(StandardError) do
              client.tickets(&proc{})
            end
          end

          test "503 with Retry-After" do
            after = "123"
            stub_response(503, ["Retry-After: #{after}"])
            mock(client).sleep after.to_i
            assert_throw(:retry) do
              client.tickets(&proc{})
            end
          end

          test "Unhandled response code (555)" do
            stub_response(555)
            assert_raise(RuntimeError.new("Server returns unknown status code (555)")) do
              client.tickets(&proc{})
            end
          end
        end

        sub_test_case ".validate_target" do
          data do
            [
              ["tickets", ["tickets", nil]],
              ["ticket_events", ["ticket_events", nil]],
              ["users", ["users", nil]],
              ["organizations", ["organizations", nil]],
              ["unknown", ["unknown", Embulk::ConfigError]],
            ]
          end
          test "validate with target" do |data|
            target, error = data
            client = Client.new({target: target})

            if error
              assert_raise(error) do
                client.validate_target
              end
            else
              assert_nothing_raised do
                client.validate_target
              end
            end
          end
        end

        def login_url
          "http://example.com"
        end

        def username
          "foo@example.com"
        end

        def password
          "passWORD"
        end

        def token
          "TOKEN"
        end

        def access_token
          "ACCESS_TOKEN"
        end
      end
    end
  end
end
