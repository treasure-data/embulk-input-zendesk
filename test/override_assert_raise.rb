module OverrideAssertRaise
  # NOTE: Embulk 0.7.1+ required to raise ConfigError to do as `ConfigError.new("message")`,
  #       original `assert_raise` method can't catch that, but `begin .. rescue` can.
  #       So we override assert_raise as below.
  def assert_raise(expected_class = StandardError, &block)
    begin
      block.call
      assert_equal expected_class, nil
    rescue ::Test::Unit::AssertionFailedError => e
      # failed assert raises this Error and that extends StandardError, so rescue it first
      raise e
    rescue expected_class.class => e
      # https://github.com/test-unit/test-unit/issues/94
      assert_equal e.message, expected_class.message
    rescue expected_class
      assert true # passed
    rescue => e
      assert_equal(expected_class, e.class) # not expected one raised
    end
  end
end
