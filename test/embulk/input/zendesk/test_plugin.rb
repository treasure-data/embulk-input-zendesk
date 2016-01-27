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

          test "invoke Client#validate_config" do
            any_instance_of(Client) do |klass|
              mock(klass).validate_config
            end
            Plugin.transaction(config("valid_auth_oauth.yml"), &@control)
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

          test "invoke Client#validate_config" do
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
            mock(@client).validate_config
            Plugin.guess(config)["columns"]
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
            assert actual.include?(name: "has_incidents", type: :boolean)

            # TODO: re-enable these json type tests after this plugin officially support it
            # assert actual.include?(name: "tags", type: :json)
            # assert actual.include?(name: "collaborator_ids", type: :json)

            # assert actual.include?(name: "custom_fields", type: :json)
            # assert actual.include?(name: "satisfaction_rating", type: :json)
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
              mock(@client).export(anything, "tickets", anything) { [] }
              mock(@client).incremental_export.never
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
              mock(@client).export.never
              mock(@client).incremental_export(anything, "tickets", 0, []) { [] }
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

            sub_test_case "config diff" do
              def end_time
                1234567890
              end

              def next_start_time
                Time.at(end_time + 1).strftime("%F %T+0900")
              end

              def start_time
                Time.at(1111111111).strftime("%F %T+0900")
              end

              setup do
                events = [
                  {"id" => 1, "created_at" => "2000-01-01T00:00:00+0900"},
                  {"id" => 2, "created_at" => "2000-01-01T01:00:00+0900"},
                ]

                @httpclient.test_loopback_http_response << [
                  "HTTP/1.1 200",
                  "Content-Type: application/json",
                  "",
                  {
                    ticket_events: events,
                    end_time: end_time,
                  }.to_json
                ].join("\r\n")
                stub(page_builder).add(anything)
                stub(page_builder).finish
              end

              sub_test_case "incremental: true" do
                def run_task
                  task.merge(schema: schema, target: "ticket_events", incremental: true, start_time: start_time)
                end

                test "task_report contains next start_time" do
                  report = @plugin.run
                  assert_equal next_start_time, report[:start_time]
                end
              end

              sub_test_case "incremental: false" do
                def run_task
                  task.merge(schema: schema, target: "ticket_events", incremental: false, start_time: start_time)
                end

                test "task_report don't contains start_time" do
                  report = @plugin.run
                  assert_nil report[:start_time]
                end
              end
            end

            sub_test_case "casting value" do
              setup do
                stub(@plugin).preview? { false }
                @httpclient.test_loopback_http_response << [
                  "HTTP/1.1 200",
                  "Content-Type: application/json",
                  "",
                  {
                    tickets: data
                  }.to_json
                ].join("\r\n")
              end

              def schema
                [
                  {"name" => "target_l", "type" => "long"},
                  {"name" => "target_f", "type" => "double"},
                  {"name" => "target_str", "type" => "string"},
                  {"name" => "target_bool", "type" => "boolean"},
                  {"name" => "target_time", "type" => "timestamp"},
                ]
              end

              def data
                [
                  {
                    "id" => 1, "target_l" => "3", "target_f" => "3", "target_str" => "str",
                    "target_bool" => false, "target_time" => "2000-01-01",
                  },
                  {
                    "id" => 2, "target_l" => 4.5, "target_f" => 4.5, "target_str" => 999,
                    "target_bool" => "truthy", "target_time" => Time.parse("1999-01-01"),
                  },
                  {
                    "id" => 3, "target_l" => nil, "target_f" => nil, "target_str" => nil,
                    "target_bool" => nil, "target_time" => nil,
                  },
                ]
              end

              test "cast as given type" do
                mock(page_builder).add([3, 3.0, "str", false, Time.parse("2000-01-01")])
                mock(page_builder).add([4, 4.5, "999", true, Time.parse("1999-01-01")])
                mock(page_builder).add([nil, nil, nil, nil, nil])
                mock(page_builder).finish

                @plugin.run
              end
            end

            sub_test_case "start_time option not given" do
              test "Nothing passed to client" do
                stub(page_builder).finish

                mock(@client).tickets(false)
                @plugin.run
              end
            end

            sub_test_case "start_time option given" do
              def run_task
                task.merge({
                  start_time: "2000-01-01T00:00:00+0000",
                  schema: schema,
                  retry_limit: 1,
                  retry_initial_wait_sec: 0,
                })
              end

              test "Passed to client as integer (epoch)" do
                stub(page_builder).finish

                start_time = Time.parse(run_task[:start_time]).to_i
                mock(@client).tickets(false, start_time)
                @plugin.run
              end
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
