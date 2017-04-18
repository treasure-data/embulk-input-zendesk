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
              JSON.parse(fixture_load("tickets.json")).to_json
            ].join("\r\n")
            mock(@client).validate_config
            Plugin.guess(config)["columns"]
          end

          test "guessing" do
            @httpclient.test_loopback_http_response << [
              "HTTP/1.1 200",
              "Content-Type: application/json",
              "",
              JSON.parse(fixture_load("tickets.json")).to_json
            ].join("\r\n")
            actual = Plugin.guess(config)["columns"]
            assert actual.include?(name: "url", type: :string)
            assert actual.include?(name: "id", type: :long)
            assert actual.include?(name: "created_at", type: :timestamp, format: "%Y-%m-%dT%H:%M:%S%z")
            assert actual.include?(name: "has_incidents", type: :boolean)
            assert actual.include?(name: "tags", type: :json)
            assert actual.include?(name: "collaborator_ids", type: :json)
            assert actual.include?(name: "custom_fields", type: :json)
            assert actual.include?(name: "group_id", type: :string)
            assert actual.include?(name: "satisfaction_rating", type: :json)
          end
        end

        sub_test_case "include subresources" do
          def page_builder
            @page_builder ||= Class.new do
              def add(_); end
              def finish; end
            end.new
          end

          sub_test_case "guess" do
            def task
              t = {
                type: "zendesk",
                login_url: "https://example.zendesk.com/",
                auth_method: "token",
                username: "foo@example.com",
                token: "token",
                target: "tickets",
                includes: includes,
              }
              t.delete :includes unless includes
              t
            end

            def config
              Embulk::DataSource.new(task)
            end

            def ticket
              JSON.parse(fixture_load("tickets.json"))
            end

            setup do
              @client = Client.new(task)
              stub(Client).new { @client }
              stub(@client).public_send(anything) do |*args|
                args.last.call(ticket)
              end
            end

            sub_test_case "includes present" do
              def includes
                %w(audits comments)
              end

              test "guessed includes fields" do
                actual = Plugin.guess(config)["columns"]
                assert actual.include?(name: "audits", type: :json)
                assert actual.include?(name: "comments", type: :json)
              end
            end

            sub_test_case "includes blank" do
              def includes
                nil
              end

              test "not guessed includes fields" do
                actual = Plugin.guess(config)["columns"]
                assert !actual.include?(name: "audits", type: :json)
                assert !actual.include?(name: "comments", type: :json)
              end
            end
          end

          sub_test_case "#run" do
            def schema
              [
                {"name" => "id", "type" => "long"},
                {"name" => "tags", "type" => "json"},
              ]
            end

            def run_task
              task.merge({
                schema: schema,
                retry_limit: 1,
                retry_initial_wait_sec: 0,
                includes: includes,
              })
            end

            setup do
              @client = Client.new(run_task)
              stub(@client).public_send {|*args| args.last.call({}) }
              @plugin = Plugin.new(run_task, nil, nil, page_builder)
              stub(@plugin).client { @client }
              @httpclient = @client.httpclient
              stub(@client).httpclient { @httpclient }
            end

            sub_test_case "preview" do
              setup do
                stub(@plugin).preview? { true }
              end

              sub_test_case "includes present" do
                def includes
                  %w(foo bar)
                end

                test "call fetch_subresource" do
                  includes.each do |ent|
                    mock(@client).fetch_subresource(anything, anything, ent)
                  end
                  @plugin.run
                end
              end

              sub_test_case "includes blank" do
                def includes
                  []
                end

                test "don't call fetch_subresource" do
                  mock(@client).fetch_subresource.never
                  @plugin.run
                end
              end
            end

            sub_test_case "run" do
              setup do
                stub(@plugin).preview? { false }
              end

              sub_test_case "includes present " do
                def includes
                  %w(foo bar)
                end

                test "call fetch_subresource" do
                  includes.each do |ent|
                    mock(@client).fetch_subresource(anything, anything, ent).at_least(1)
                  end
                  @plugin.run
                end
              end

              sub_test_case "includes blank" do
                def includes
                  []
                end

                test "don't call fetch_subresource" do
                  mock(@client).fetch_subresource.never
                  @plugin.run
                end
              end
            end
          end
        end

        sub_test_case "#run" do
          def page_builder
            @page_builder ||= Object.new
          end

          def schema
            [
              {"name" => "id", "type" => "long"},
              {"name" => "tags", "type" => "json"},
            ]
          end

          def run_task
            task.merge({
              schema: schema,
              retry_limit: 1,
              retry_initial_wait_sec: 0,
              includes: [],
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
              mock(@client).export.never
              mock(@client).incremental_export(anything, "tickets", anything, anything, anything) { [] }
              mock(page_builder).finish

              @plugin.run
            end

            test "task[:schema] columns passed into page_builder.add" do
              tickets = [
                {"id" => 1, "created_at" => "2000-01-01T00:00:00+0900", "tags" => ["foo"]},
                {"id" => 2, "created_at" => "2000-01-01T00:00:00+0900", "tags" => ["foo"]},
              ]

              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: tickets
                }.to_json
              ].join("\r\n")

              first_ticket = tickets[0]
              second_ticket = tickets[1]
              mock(page_builder).add([first_ticket["id"], first_ticket["tags"]])
              mock(page_builder).add([second_ticket["id"], second_ticket["tags"]]).never
              mock(page_builder).finish

              @plugin.run
            end
          end

          sub_test_case "run" do
            setup do
              stub(@plugin).preview? { false }
              stub(Embulk).logger { Logger.new(File::NULL) }
            end

            test "call ticket_all method instead of tickets" do
              mock(@client).export.never
              mock(@client).incremental_export(anything, "tickets", 0, [], false) { [] }
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
                  tickets: tickets,
                  count: tickets.length,
                }.to_json
              ].join("\r\n")

              tickets.each do |ticket|
                # schema[:columns] is id and tags. tags should be nil
                mock(page_builder).add([ticket["id"], nil])
              end
              mock(page_builder).finish

              @plugin.run
            end

            sub_test_case "config diff" do
              def end_time
                1234567890
              end

              def next_start_time
                Time.at(end_time + 1).strftime("%F %T%z")
              end

              def start_time
                Time.at(1111111111).strftime("%F %T%z")
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
                    count: events.length,
                  }.to_json
                ].join("\r\n")
                stub(page_builder).add(anything)
                stub(page_builder).finish
                stub(Embulk).logger { Logger.new(File::NULL) }
              end

              sub_test_case "incremental: true" do
                def run_task
                  task.merge(schema: schema, target: "ticket_events", incremental: true, start_time: start_time)
                end

                test "task_report contains next start_time" do
                  report = @plugin.run
                  assert_equal next_start_time, report[:start_time]
                end

                test "no record" do
                  first_report = @plugin.run

                  @httpclient.test_loopback_http_response << [
                    "HTTP/1.1 200",
                    "Content-Type: application/json",
                    "",
                    {
                      ticket_events: [],
                      count: 0,
                    }.to_json
                  ].join("\r\n")
                  second_report = @plugin.run

                  assert second_report.has_key?(:start_time)
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
                stub(Embulk).logger { Logger.new(File::NULL) }
                stub(@plugin).preview? { false }
                @httpclient.test_loopback_http_response << [
                  "HTTP/1.1 200",
                  "Content-Type: application/json",
                  "",
                  {
                    tickets: data,
                    count: data.length,
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
                  {"name" => "target_json", "type" => "json"},
                ]
              end

              def data
                [
                  {
                    "id" => 1, "target_l" => "3", "target_f" => "3", "target_str" => "str",
                    "target_bool" => false, "target_time" => "2000-01-01",
                    "target_json" => [1,2,3],
                  },
                  {
                    "id" => 2, "target_l" => 4.5, "target_f" => 4.5, "target_str" => 999,
                    "target_bool" => "truthy", "target_time" => Time.parse("1999-01-01"),
                    "target_json" => {"foo" => "bar"},
                  },
                  {
                    "id" => 3, "target_l" => nil, "target_f" => nil, "target_str" => nil,
                    "target_bool" => nil, "target_time" => nil,
                    "target_json" => nil,
                  },
                ]
              end

              test "cast as given type" do
                mock(page_builder).add([3, 3.0, "str", false, Time.parse("2000-01-01"), [1,2,3]])
                mock(page_builder).add([4, 4.5, "999", true, Time.parse("1999-01-01"), {"foo" => "bar"}])
                mock(page_builder).add([nil, nil, nil, nil, nil, nil])
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

          sub_test_case "flush each 10k records" do
            setup do
              stub(Embulk).logger { Logger.new(File::NULL) }
              stub(@plugin).preview? { false }
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: (1..20000).map { |i| { 'id' => i } },
                  count: 20000,
                  end_time: 0,
                }.to_json
              ].join("\r\n")
              # to stop pagination (count < 1000)
              @httpclient.test_loopback_http_response << [
                "HTTP/1.1 200",
                "Content-Type: application/json",
                "",
                {
                  tickets: [{ 'id' => 20001 }],
                  count: 1,
                  end_time: 0,
                }.to_json
              ].join("\r\n")
            end

            test "flush is called twice" do
              mock(page_builder).add(anything).times(20001)
              mock(page_builder).flush.times(2)
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
