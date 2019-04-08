Embulk::JavaPlugin.register_input(
  "zendesk", "org.embulk.input.zendesk.ZendeskInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
