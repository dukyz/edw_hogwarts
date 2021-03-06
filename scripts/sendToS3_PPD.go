package main
import "os/exec"
import "os"

func main() {

    part_val := "export bucket=cnn-s3-rg-edw && export http_proxy=http://username:password@ip:port && export https_proxy=http://username:password@ip:port"

    part_files := "export tmp_file=`uuidgen`.parquet && export file_name=`date +'%F_%T.%3N'`.parquet && export schema_file=" + os.Args[1]

    part_name := "export domain_name=`grep \"domain_name\" ${schema_file} | awk -F : '{print $2}' | awk -F \\\" '{print $2}'`"

    part_pre := "export JAVA_HOME=/apps/jdk1.8.0_201 && export PATH=$JAVA_HOME/bin:$PATH"

    part_exec := "java -cp '/apps/parquet/lib/*' org.apache.parquet.cli.Main convert-csv stdin -o ${tmp_file} -s ${schema_file} --overwrite && aws s3 cp ${tmp_file} s3://${bucket}/_input/${domain_name}/${file_name}"

    part_post := " : "

    part_clean := "rm -f ${tmp_file} .${tmp_file}.crc"

    shell_script := part_val + " && " + part_files + " && " + part_name + " && " + part_pre + " && " + part_exec + " && " + part_post + " ; " + part_clean

    cmd := exec.Command("/bin/bash","-c",shell_script)
    cmd.Stderr = os.Stdout
    cmd.Stdout = os.Stdout
    cmd.Stdin = os.Stdin
    cmd.Run()
}
