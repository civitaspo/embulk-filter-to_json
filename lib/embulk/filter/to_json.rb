Embulk::JavaPlugin.register_filter(
  "to_json", "org.embulk.filter.to_json.ToJsonFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
