require "pathname"

module FixtureHelper
  def fixture_dir
    Pathname.new(__FILE__).dirname.join("fixtures")
  end

  def fixture_load(name)
    fixture_dir.join(name).read
  end
end
