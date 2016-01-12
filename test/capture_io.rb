module CaptureIo
  def capture(output = :out, &block)
    _, out = swap_io(output, &block)
    out
  end

  def silence(&block)
    block_result = nil
    swap_io(:out) do
      block_result,_ = swap_io(:err, &block)
    end
    block_result
  end

  def swap_io(output = :out, &block)
    java_import 'java.io.PrintStream'
    java_import 'java.io.ByteArrayOutputStream'
    java_import 'java.lang.System'

    ruby_original_stream = output == :out ? $stdout.dup : $stderr.dup
    java_original_stream = System.send(output) # :out or :err
    ruby_buf = StringIO.new
    java_buf = ByteArrayOutputStream.new

    case output
    when :out
      $stdout = ruby_buf
      System.setOut(PrintStream.new(java_buf))
    when :err
      $stderr = ruby_buf
      System.setErr(PrintStream.new(java_buf))
    end

    [block.call, ruby_buf.string + java_buf.toString]
  ensure
    case output
    when :out
      $stdout = ruby_original_stream
      System.setOut(java_original_stream)
    when :err
      $stderr = ruby_original_stream
      System.setErr(java_original_stream)
    end
  end
end
