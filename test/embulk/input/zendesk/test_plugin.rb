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
            omit "Can't rescue correctly :("

            assert_raise(ConfigError) do
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
        end
      end
    end
  end
end
