
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-zendesk"
  spec.version       = "0.2.5"
  spec.authors       = ["uu59", "muga", "sakama"]
  spec.summary       = "Zendesk input plugin for Embulk"
  spec.description   = "Loads records from Zendesk."
  spec.email         = ["k@uu59.org", "muga.nishizawa@gmail.com", "satoshiakama@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/treasure-data/embulk-input-zendesk"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'perfect_retry', '~> 0.5'
  spec.add_dependency 'httpclient'
  spec.add_dependency 'concurrent-ruby'
  spec.add_development_dependency 'embulk', ['~> 0.8.1']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'test-unit', '~> 3.1.5'
  spec.add_development_dependency 'test-unit-rr'
  spec.add_development_dependency 'rr', '~> 1.1.2'
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency 'gem_release_helper', "~> 1.0"
  spec.add_development_dependency "codeclimate-test-reporter", "~> 0.6"
end
