require "bundler/gem_tasks"
require "everyleaf/embulk_helper/tasks"

task default: :test

desc "Run tests"
task :test do
  ruby("--debug", "test/run-test.rb", "--use-color=yes", "--collector=dir")
end

desc "Run tests with coverage"
task :cov do
  ENV["COVERAGE"] = "1"
  ruby("--debug", "test/run-test.rb", "--use-color=yes", "--collector=dir")
end


Everyleaf::EmbulkHelper::Tasks.install(
  gemspec: "./embulk-input-zendesk.gemspec",
  github_name: "treasure-data/embulk-input-zendesk",
)
