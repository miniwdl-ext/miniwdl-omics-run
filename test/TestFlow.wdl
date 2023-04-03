version 1.0

# Adapted from:
#  https://docs.aws.amazon.com/omics/latest/dev/setting-up-workflows.html

workflow TestFlow {
    input {
        File input_txt_file
        String docker
    }

    #copies input file data to output. 
    call TxtFileCopyTask{
        input:
            input_txt_file = input_txt_file,
            docker = docker
    }

    output {
        File output_txt_file = TxtFileCopyTask.output_txt_file
    }

}

#Task Definitions
task TxtFileCopyTask {
    input {
        File input_txt_file
        String docker
    }

    command {
        cat ~{input_txt_file} > outfile.txt
    }

    output {
        File output_txt_file = "outfile.txt"
    }

    runtime {
        cpu: 2
        memory: "4 GiB"
        docker: docker
    }
}
