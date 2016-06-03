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

        sub_test_case "tickets (incremental export)" do
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
          end
        end

        sub_test_case "ticket_metrics (non-incremental export)" do
          sub_test_case "partial" do
            def client
              @client ||= Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token, retry_limit: 1, retry_initial_wait_sec: 0)
            end

            setup do
              stub(Embulk).logger { Logger.new(File::NULL) }
              @httpclient = client.httpclient
              stub(client).httpclient { @httpclient }
            end

            test "fetch ticket_metrics first page only" do
              records = [
                {"id" => 1},
                {"id" => 2},
              ]
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  ticket_metrics: records,
                  next_page: "https://treasuredata.zendesk.com/api/v2/ticket_metrics.json?page=2",
                }.to_json
              ].join("\r\n")

              handler = proc { }
              records.each do |record|
                mock(handler).call(record)
              end
              client.ticket_metrics(true, &handler)
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

            test "fetch ticket_metrics all page" do
              records = 100.times.map{|n| {"id"=> n}}
              second_results = [
                {"id" => 101}
              ]
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  ticket_metrics: records,
                  count: records.length + second_results.length,
                  next_page: "https://treasuredata.zendesk.com/api/v2/ticket_metrics.json?page=2",
                }.to_json
              ].join("\r\n")

              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  ticket_metrics: second_results,
                  count: records.length + second_results.length,
                  next_page: nil,
                }.to_json
              ].join("\r\n")

              handler = proc { }
              records.each do |record|
                mock(handler).call(record)
              end
              second_results.each do |record|
                mock(handler).call(record)
              end
              client.ticket_metrics(false, &handler)
            end

            test "fetch tickets without duplicated" do
              records = [
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
                  ticket_metrics: records,
                  count: records.length,
                }.to_json
              ].join("\r\n")

              handler = proc { }
              mock(handler).call(anything).twice
              client.ticket_metrics(false, &handler)
            end

            test "fetch tickets with next_page" do
              end_time = Time.now.to_i

              response_1 = [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  ticket_metrics: 100.times.map{|n| {"id" => n}},
                  count: 101,
                  next_page: "https://treasuredata.zendesk.com/api/v2/ticket_metrics.json?page=2",
                }.to_json
              ].join("\r\n")

              response_2 = [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  ticket_metrics: [{"id" => 101}],
                  count: 101,
                }.to_json
              ].join("\r\n")

              @httpclient.test_loopback_http_response << response_1
              @httpclient.test_loopback_http_response << response_2

              handler = proc { }
              mock(handler).call(anything).times(101)
              client.ticket_metrics(false, &handler)
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
              mock(client).incremental_export(anything, "ticket_events", anything, [], true)
              client.ticket_events(true)
            end

            test "invoke incremental_export when partial=false" do
              mock(client).incremental_export(anything, "ticket_events", anything, [], false)
              client.ticket_events(false)
            end
          end

          sub_test_case "ticket_fields" do
            test "invoke export when partial=true" do
              mock(client).export(anything, "ticket_fields")
              client.ticket_fields(true)
            end

            test "invoke export when partial=false" do
              mock(client).export_parallel(anything, "ticket_fields")
              client.ticket_fields(false)
            end
          end

          sub_test_case "ticket_forms" do
            test "invoke export when partial=true" do
              mock(client).export(anything, "ticket_forms")
              client.ticket_forms(true)
            end

            test "invoke export when partial=false" do
              mock(client).export_parallel(anything, "ticket_forms")
              client.ticket_forms(false)
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

          def stub_response(status, headers = [], body_json = nil)
            headers << "Content-Type: application/json"
            @httpclient.test_loopback_http_response << [
              "HTTP/1.1 #{status}",
              headers.join("\r\n"),
              "",
              body_json || {
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

          test "403 forbidden" do
            stub_response(403)
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
            error_body = {error: "FATAL ERROR"}.to_json
            stub_response(555, [], error_body)
            assert_raise(RuntimeError.new("Server returns unknown status code (555) #{error_body}")) do
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

        sub_test_case ".extract_valid_json_from_chunk" do
          setup do
            @client = Client.new({target: "tickets"})
          end

          test "complete json" do
            actual = @client.send(:extract_valid_json_from_chunk, '{"tickets":[{"foo":1},{"foo":2}]}')
            assert_equal ['{"foo":1}', '{"foo":2}'], actual
          end

          test "broken json" do
            json = '{"ticket_events":[{"foo":1},{"foo":2},{"fo'
            actual = @client.send(:extract_valid_json_from_chunk, json)
            expected = [
              '{"foo":1}',
              '{"foo":2}',
            ]
            assert_equal expected, actual
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
