require "perfect_retry"

module Embulk
  module Input
    module Zendesk
      class Plugin < InputPlugin
        ::Embulk::Plugin.register_input("zendesk", self)

        def self.transaction(config, &control)
          task = config_to_task(config)
          Client.new(task).validate_credentials
          validate_target(task[:target])

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

        def self.guess(config)
          task = config_to_task(config)
          client = Client.new(task)
          client.validate_credentials
          validate_target(task[:target])

          records = []
          method = determine_export_method(task[:target], true)
          client.send(method) do |record|
            records << record
          end

          columns = Guess::SchemaGuess.from_hash_records(records).map do |column|
            hash = column.to_h
            hash.delete(:index)
            hash.delete(:format) unless hash[:format]

            # NOTE: Embulk 0.8.1 guesses Hash and Hashes in Array as string.
            #       https://github.com/embulk/embulk/issues/379
            #       This is workaround for that
            if records.any? {|r| [Array, Hash].include?(r[hash[:name]].class) }
              hash[:type] = :json
            end

            hash
          end

          return {"columns" => columns}
        end

        def self.config_to_task(config)
          {
            login_url: config.param("login_url", :string),
            auth_method: config.param("auth_method", :string, default: "basic"),
            target: config.param("target", :string),
            username: config.param("username", :string, default: nil),
            password: config.param("password", :string, default: nil),
            token: config.param("token", :string, default: nil),
            access_token: config.param("access_token", :string, default: nil),
            retry_limit: config.param("retry_limit", :integer, default: 5),
            retry_initial_wait_sec: config.param("retry_initial_wait_sec", :integer, default: 1),
            schema: config.param(:columns, :array, default: []),
          }
        end

        def init
        end

        def run
          client = Client.new(task)
          method = self.class.determine_export_method(task[:target], preview?)

          client.send(method) do |ticket|
            values = extract_values(ticket)
            page_builder.add(values)
          end

          page_builder.finish

          task_report = {}
          return task_report
        end

        private

        def self.validate_target(target)
          unless determine_export_method(target)
            raise Embulk::ConfigError.new("target: #{target} is not supported.")
          end
        end

        def self.determine_export_method(target, partial = true)
          # NOTE: incremental export API for `embulk run`, otherwise such `embulk preview` and `embulk guess` use export API
          #       Because incremental export API returns 1000 records per page but it is too large and too slow to guess/preview.
          case target
          when "tickets"
            partial ? :tickets : :ticket_all
          when "ticket_events"
            # NOTE: ticket_events only have full export API
            :ticket_events
          when "users"
            partial ? :users : :user_all
          when "organizations"
            partial ? :organizations : :organization_all
          end
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
