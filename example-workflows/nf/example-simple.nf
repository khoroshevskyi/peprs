#!/usr/bin/env nextflow

// Define parameters
params.greeting = "Hello"
params.name = "World"

// Display help message
if (params.help) {
    log.info """
    Simple Greeting Workflow
    ========================
    Usage:
        nextflow run example-simple.nf --greeting "Hi" --name "Alice"
    
    Parameters:
        --greeting    Greeting message (default: "Hello")
        --name        Name to greet (default: "World")
        --help        Show this help message
    """
    exit 0
}

// Create a channel with the greeting parameters
greeting_ch = Channel.of("${params.greeting} ${params.name}!")

// Define the greeting process
process sayGreeting {
    input:
    val greeting_text from greeting_ch
    
    output:
    stdout into result_ch
    
    script:
    """
    echo "${greeting_text}"
    """
}

// Display the result
result_ch.view { "Greeting: $it" }