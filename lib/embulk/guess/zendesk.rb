require 'json'

module Embulk
  module Guess
    class ZendeskGuess < TextGuessPlugin
      Plugin.register_guess("zendesk", self)

      def guess_text(config, sample_text)
        {:columns =>
             SchemaGuess.from_hash_records(JSON.parse(sample_text)).map do |c|
               {
                   name: c.name,
                   type: c.type,
                   **(c.format ? {format: c.format} : {})
               }
             end
        }
      end
    end
  end
end 