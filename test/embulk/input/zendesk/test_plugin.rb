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
      class TestPlugin < Test::Unit::TestCase
        include OverrideAssertRaise
        include FixtureHelper
        include CaptureIo

        def run_with(yml)
          silence do
            Embulk::Runner.run(YAML.load fixture_load(yml))
          end
        end

        sub_test_case "exec" do
          setup do
            stub(Plugin).resume { Hash.new }
          end

          test "run with valid.yml (basic)" do
            assert_nothing_raised do
              run_with("valid_auth_basic.yml")
            end
          end

          test "run with valid.yml (token)" do
            assert_nothing_raised do
              run_with("valid_auth_token.yml")
            end
          end

          test "run with valid.yml (oauth)" do
            assert_nothing_raised do
              run_with("valid_auth_oauth.yml")
            end
          end

          test "run with invalid username lack" do
            # NOTE: will be raised Java::OrgEmbulkExec::PartialExecutionException, not ConfigError. It is Embulk internally exception handling matter.
            assert_raise do
              run_with("invalid_lack_username.yml")
            end
          end
        end

        sub_test_case ".transaction" do
          setup do
            stub(Plugin).resume { Hash.new }
            @control = proc { Hash.new }
          end

          def config(yml)
            conf = YAML.load fixture_load(yml)
            Embulk::DataSource.new(conf["in"])
          end

          test "lack username config" do
            assert_raise(ConfigError) do
              Plugin.transaction(config("invalid_lack_username.yml"), &@control)
            end
          end

          test "unknown auth_method" do
            assert_raise(ConfigError) do
              Plugin.transaction(config("invalid_unknown_auth.yml"), &@control)
            end
          end

          test "run as well" do
            actual = nil
            assert_nothing_raised do
              actual = Plugin.transaction(config("valid_auth_oauth.yml"), &@control)
            end

            expected = {}
            assert_equal expected, actual
          end
        end

        sub_test_case ".guess" do
          setup do
            @client = Client.new(task)
            stub(Client).new { @client }
            @httpclient = @client.httpclient
            stub(@client).httpclient { @httpclient }
          end

          test "guessing" do
            @httpclient.test_loopback_http_response << [
              "HTTP/1.1 200",
              "Content-Type: application/json",
              "",
              {
                tickets: [
                  JSON.parse(fixture_load("tickets.json"))
                ]
              }.to_json
            ].join("\r\n")
            actual = Plugin.guess(config)["columns"]
            assert actual.include?(name: "url", type: :string)
            assert actual.include?(name: "id", type: :long)
            assert actual.include?(name: "created_at", type: :timestamp, format: "%Y-%m-%dT%H:%M:%S%z")
            assert actual.include?(name: "collaborator_ids", type: :json)
            assert actual.include?(name: "has_incidents", type: :boolean)
            assert actual.include?(name: "tags", type: :json)

            assert actual.include?(name: "custom_fields", type: :json)
            assert actual.include?(name: "satisfaction_rating", type: :json)
          end
        end

        sub_test_case "#run" do
          def page_builder
            @page_builder ||= Object.new
          end

          def schema
            [
              {"name" => "id", "type" => "long"}
            ]
          end

          def run_task
            task.merge({
              schema: schema,
              retry_limit: 1,
              retry_initial_wait_sec: 0,
            })
          end

          setup do
            @client = Client.new(run_task)
            stub(Client).new { @client }
            @httpclient = @client.httpclient
            stub(@client).httpclient { @httpclient }
            @plugin = Plugin.new(run_task, nil, nil, page_builder)
          end

          sub_test_case "preview" do
            setup do
              stub(@plugin).preview? { true }
            end

            test "call tickets method instead of ticket_all" do
              mock(@client).tickets { [] }
              mock(@client).ticket_all.never
              mock(page_builder).finish

              @plugin.run
            end

            test "task[:schema] columns passed into page_builder.add" do
              tickets = [
                {"id" => 1, "created_at" => "2000-01-01T00:00:00+0900"},
                {"id" => 2, "created_at" => "2000-01-01T00:00:00+0900"},
              ]

              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: tickets
                }.to_json
              ].join("\r\n")

              tickets.each do |ticket|
                mock(page_builder).add([ticket["id"]])
              end
              mock(page_builder).finish

              @plugin.run
            end
          end

          sub_test_case "run" do
            setup do
              stub(@plugin).preview? { false }
            end

            test "call ticket_all method instead of tickets" do
              mock(@client).tickets.never
              mock(@client).ticket_all { [] }
              mock(page_builder).finish

              @plugin.run
            end

            test "task[:schema] columns passed into page_builder.add" do
              tickets = [
                {"id" => 1, "created_at" => "2000-01-01T00:00:00+0900"},
                {"id" => 2, "created_at" => "2000-01-01T00:00:00+0900"},
              ]

              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: tickets
                }.to_json
              ].join("\r\n")

              tickets.each do |ticket|
                mock(page_builder).add([ticket["id"]])
              end
              mock(page_builder).finish

              @plugin.run
            end
          end
        end

        def yml
          "valid_auth_basic.yml"
        end

        def config
          conf = YAML.load fixture_load(yml)
          Embulk::DataSource.new(conf["in"])
        end

        def task
          config.to_h.each_with_object({}) do |(k,v), result|
            result[k.to_sym] = v
          end
        end
      end
    end
  end
end
