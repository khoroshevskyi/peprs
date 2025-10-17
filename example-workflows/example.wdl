version 1.0

# Define a "struct" that represents the data for one sample.
# This matches the structure of the objects inside the JSON array you're making!
struct Sample {
  File file
  String protocol
}

workflow say_hello {
  input {
    # The workflow's main input is an ARRAY of our Sample struct.
    Array[Sample] samples_array
  }

  # Scatter over the input array. The code inside this block
  # will run for each sample in the `samples_array`.
  scatter (sample in samples_array) {
    call greet {
      input:
        in_file = sample.file,
        in_protocol = sample.protocol
    }
  }
}

task greet {
  input {
    File in_file
    String in_protocol
  }

  command <<<
    echo "Processing file ~{in_file} with protocol ~{in_protocol}"
  >>>

  output {
    String result = read_string(stdout())
  }
}