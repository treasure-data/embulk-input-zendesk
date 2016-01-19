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
          def client
            @client ||= Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token, retry_limit: 1, retry_wait_initial_sec: 0)
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

        sub_test_case "ticket_all" do
          def client
            @client ||= Client.new(login_url: login_url, auth_method: "oauth", access_token: access_token, retry_limit: 1, retry_wait_initial_sec: 0)
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
            client.ticket_all(&handler)
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
            client.ticket_all(&handler)
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
            client.ticket_all(&handler)
          end

          test "raise DataError when invalid JSON response" do
            @httpclient.test_loopback_http_response << [
              "HTTP/1.1 200",
              "Content-Type: application/json",
              "",
              "[[[" # invalid json
            ].join("\r\n")

            assert_raise(DataError) do
              client.ticket_all
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
