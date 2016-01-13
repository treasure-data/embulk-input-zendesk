require "perfect_retry"

module Embulk
  module Input
    module Zendesk
      class Plugin < InputPlugin
        ::Embulk::Plugin.register_input("zendesk", self)

        def self.transaction(config, &control)
          task = config_to_task(config)
          Client.new(task).validate_credentials

          columns = task[:schema].map do |column|
            name = column["name"]
            type = column["type"].to_sym

            Column.new(nil, name, type, column["format"])
          end

          resume(task, columns, 1, &control)
        end

        def self.resume(task, columns, count, &control)
          task_reports = yield(task, columns, count)

          next_config_diff = {}
          return next_config_diff
        end

        # TODO
        #def self.guess(config)
        #  sample_records = [
        #    {"example"=>"a", "column"=>1, "value"=>0.1},
        #    {"example"=>"a", "column"=>2, "value"=>0.2},
        #  ]
        #  columns = Guess::SchemaGuess.from_hash_records(sample_records)
        #  return {"columns" => columns}
        #end

        def self.config_to_task(config)
          {
            login_url: config.param("login_url", :string),
            auth_method: config.param("auth_method", :string, default: "basic"),
            username: config.param("username", :string, default: nil),
            password: config.param("password", :string, default: nil),
            token: config.param("token", :string, default: nil),
            access_token: config.param("access_token", :string, default: nil),
            retry_limit: config.param("retry_limit", :integer, default: 5),
            retry_wait_initial_sec: config.param("retry_wait_initial_sec", :integer, default: 1),
            schema: config.param(:columns, :array, default: []),
          }
        end

        def init
        end

        def run
          client = Client.new(task)
          method = preview? ? :tickets : :ticket_all

          client.send(method) do |ticket|
            values = extract_values(ticket)
            page_builder.add(values)
          end

          page_builder.finish

          task_report = {}
          return task_report
        end

        def preview?
          org.embulk.spi.Exec.isPreview()
        rescue java.lang.NullPointerException => e
          false
        end

        def extract_values(ticket)
          values = task[:schema].map do |column|
            ticket[column["name"].to_s]
          end

          values
        end
      end

    end
  end
end
