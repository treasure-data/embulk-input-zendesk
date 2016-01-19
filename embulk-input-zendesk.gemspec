
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-zendesk"
  spec.version       = "0.1.0"
  spec.authors       = ["uu59"]
  spec.summary       = "Zendesk input plugin for Embulk"
  spec.description   = "Loads records from Zendesk."
  spec.email         = ["k@uu59.org"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/k/embulk-input-zendesk"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'perfect_retry', '~> 0.4.0'
  spec.add_dependency 'httpclient'
  spec.add_development_dependency 'embulk', ['~> 0.8.1']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'test-unit', '~> 3.1.5'
  spec.add_development_dependency 'test-unit-rr'
  spec.add_development_dependency 'rr', '~> 1.1.2'
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency 'everyleaf-embulk_helper'
  spec.add_development_dependency "codeclimate-test-reporter"
end
