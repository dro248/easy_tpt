"""
run_job.py
==========
This package contains all the required information to convert our SQL
to a "Teradata Parallel Transporter" script – Teradata's fast method for
exporting large amounts of data.

Process:
    1. COMPILE: our SQL + parameters are converted into jobs.tpt and variables.tpt
    2. RUN: Run the TPT script using Teradata Tools & Utilities (TTU)
"""
import logging
import os
from jinja2 import Template


def str_to_bool(val: str) -> bool:
    """
    Converts a string to a Boolean.
    Args:
        val: (str) Expects "True" or "False"
    Returns: Boolean
    """
    return True if val.lower() == "true" else False


def assert_string_not_empty(obj, obj_name: str):
    """
    Assert that a object (obj) is a string and is not empty.
    Args:
        obj:
        obj_name:
    """
    assert isinstance(
        obj, str
    ), f"Invalid input for '{obj_name}'; expected: str; received: {type(obj)}"

    assert obj, f"Invalid input for '{obj_name}'; Expected: non-empty string; Received: ''"


def fill_variables_template(
    teradata_database: str,
    teradata_user: str,
    teradata_password: str,
    field_delimiter: str,
    variables_template_path: str,
) -> str:
    """
    Fills the `variables_template.jinja` template with the values it needs.

    Args:
        teradata_user:
        teradata_password:
        teradata_database:
        field_delimiter:
        variables_template_path: the path to the variables jinja file

    Returns:
        Returns the variables.tpt file as a string (not yet written to disk)
    """
    logging.debug("Filling Variables template")

    # validate args
    assert_string_not_empty(teradata_database, "teradata_database")
    assert_string_not_empty(teradata_user, "teradata_user")
    assert_string_not_empty(teradata_password, "teradata_password")
    assert_string_not_empty(field_delimiter, "field_delimiter")
    assert_string_not_empty(variables_template_path, "variables_template")
    assert os.path.exists(
        variables_template_path
    ), f"Unable to find 'variables_template' at: {os.path.realpath(variables_template_path)}"

    try:
        # Read and fill in template values
        with open(variables_template_path, "r") as var_template_file:
            rendered_contents = Template(var_template_file.read()).render(
                teradata_user=teradata_user,
                teradata_password=teradata_password,
                teradata_database=teradata_database,
                field_delimiter=field_delimiter,
            )
            return rendered_contents

    except Exception as e:
        logging.error(
            "Failed to render the `variables_template.jinja` file from the provided args."
        )
        raise e


def fill_s3_job_template(
    job_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    s3_bucket: str,
    s3_region: str,
    s3_filepath: str,
    s3_filename: str,
    s3_dont_split_rows: bool,
    write_to_single_file: bool,
    sql: str,
    job_template_path: str,
) -> str:
    """
    Fills the `job_template.jinja` template with the values filled in.

    Args:
        job_name:
        aws_access_key_id:
        aws_secret_access_key:
        s3_bucket:
        s3_region:
        s3_filepath:
        s3_filename:
        s3_dont_split_rows:
        write_to_single_file:
        sql:
        job_template_path: the path to the job jinja file

    Returns:
        Returns the job.tpt file as a string (not yet written to disk)
    """
    logging.debug("Filling Job template")

    # validate args
    assert_string_not_empty(job_name, "job_name")
    assert_string_not_empty(aws_access_key_id, "aws_access_key_id")
    assert_string_not_empty(aws_secret_access_key, "aws_secret_access_key")
    assert_string_not_empty(s3_bucket, "s3_bucket")
    assert_string_not_empty(s3_region, "s3_region")
    assert_string_not_empty(s3_filepath, "s3_filepath")
    assert_string_not_empty(s3_filename, "s3_filename")
    assert isinstance(
        s3_dont_split_rows, bool
    ), f"Invalid input for 's3_dont_split_rows'; expected: str; received: {type(s3_dont_split_rows)}"
    assert isinstance(
        write_to_single_file, bool
    ), f"Invalid input for 'write_to_single_file'; expected: str; received: {type(write_to_single_file)}"
    assert_string_not_empty(sql, "sql")
    assert_string_not_empty(job_template_path, "job_template")
    assert os.path.exists(
        job_template_path
    ), f"Unable to find 'job_template' at: {os.path.realpath(job_template_path)}"

    try:
        # Read and fill in template values
        with open(job_template_path, "r") as job_template_file:
            rendered_contents = Template(job_template_file.read()).render(
                job_name=job_name,
                s3_bucket=s3_bucket,
                s3_filepath=s3_filepath,
                s3_filename=s3_filename,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                s3_region=s3_region,
                s3_dont_split_rows=s3_dont_split_rows,
                write_to_single_file=write_to_single_file,
                # When putting SQL into a TPT script, single quotes must be replaced with 2-single quotes
                sql=sql.replace("\n", " \n").replace("'", "''"),
            )
            return rendered_contents

    except Exception as e:
        logging.error(
            "Failed to render the `job_template.jinja` file from the provided args."
        )
        raise e


def compile_job() -> None:
    """Generates the `job.tpt` & `variables.tpt` files passed in from the environment."""
    logging.info("Compiling Job")
    cloud_storage = os.getenv("CLOUD_STORAGE", "").lower()
    assert_string_not_empty(cloud_storage, "cloud_storage")

    with open("variables.tpt", "w") as var_file:
        # Create `variables.tpt` file
        vars_text = fill_variables_template(
            teradata_database=os.getenv("TERADATA_DATABASE", ""),
            teradata_user=os.getenv("TERADATA_USER", ""),
            teradata_password=os.getenv("TERADATA_PASSWORD", ""),
            field_delimiter=os.getenv("FIELD_DELIMITER", "|"),
            variables_template_path="templates/variables_template.jinja",
        )
        # Write `vars_text` to `variables.tpt` (var_file)
        var_file.write(vars_text)

    if cloud_storage == "s3":
        with open("job.tpt", "w") as job_file:
            # Create `job.tpt` file
            job_text = fill_s3_job_template(
                job_name=os.getenv("JOB_NAME", ""),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
                s3_bucket=os.getenv("S3_BUCKET", ""),
                s3_region=os.getenv("S3_REGION", ""),
                s3_filepath=os.getenv("S3_FILEPATH", ""),
                s3_filename=os.getenv("S3_FILENAME", ""),
                s3_dont_split_rows=str_to_bool(os.getenv("S3_DONT_SPLIT_ROWS", "True")),
                write_to_single_file=str_to_bool(
                    os.getenv("WRITE_TO_SINGLE_FILE", "False")
                ),
                sql=os.getenv("SQL", ""),
                job_template_path="templates/s3_job_template.jinja",
            )
            # Write `job_text` to `job.tpt` (job_file)
            job_file.write(job_text)


if __name__ == "__main__":
    import subprocess

    # Compile job
    compile_job()

    # Run job
    subprocess.run("tbuild -f job.tpt -v variables.tpt".split())
