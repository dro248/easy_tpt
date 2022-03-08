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

    assert (
        obj
    ), f"Invalid input for '{obj_name}'; Expected: non-empty string; Received: ''"


def fill_teradata_creds_template(
    teradata_database: str,
    teradata_user: str,
    teradata_password: str,
    field_delimiter: str,
    teradata_creds_template_path: str,
) -> str:
    """
    Fills the `teradata_creds_template.jinja` template with the values it needs.

    Args:
        teradata_user:
        teradata_password:
        teradata_database:
        field_delimiter:
        teradata_creds_template_path: the path to the teradata_creds jinja file

    Returns:
        Returns the teradata_creds.tpt file as a string (not yet written to disk)
    """
    logging.debug("Filling Variables template")

    # validate args
    assert_string_not_empty(teradata_database, "teradata_database")
    assert_string_not_empty(teradata_user, "teradata_user")
    assert_string_not_empty(teradata_password, "teradata_password")
    assert_string_not_empty(field_delimiter, "field_delimiter")
    assert_string_not_empty(teradata_creds_template_path, "teradata_creds_template")
    assert os.path.exists(
        teradata_creds_template_path
    ), f"Unable to find 'variables_template' at: {os.path.realpath(teradata_creds_template_path)}"

    try:
        # Read and fill in template values
        with open(teradata_creds_template_path, "r") as tc_template_file:
            rendered_contents = Template(tc_template_file.read()).render(
                teradata_user=teradata_user,
                teradata_password=teradata_password,
                teradata_database=teradata_database,
                field_delimiter=field_delimiter,
            )
            return rendered_contents

    except Exception as e:
        logging.error(
            "Failed to render the `teradata_creds_template.jinja` file from the provided args."
        )
        raise e


#################
#   S3 JOB      #
#################
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


#################
#   AZURE JOB   #
#################
def fill_azure_job_template(
    job_name: str,
    container_name: str,
    file_prefix: str,
    filename: str,
    write_to_single_file: bool,
    dont_split_rows: bool,
    creds_dir: str,
    sql: str,
    job_template_path: str,
) -> str:
    """
    Fills the `job_template.jinja` (Azure) template with the values filled in.

    Args:
        job_name:
        container_name: name of the Azure container
        file_prefix: path of the file
        filename:
        write_to_single_file:
        dont_split_rows:
        creds_dir: directory (inside the Docker container) where the `credentials` file lives
        sql:
        job_template_path: the path to the job jinja file

    Returns:
        Returns the job.tpt file as a string (not yet written to disk)
    """
    logging.debug("Filling Job template")

    # validate args
    assert_string_not_empty(job_name, "job_name")
    assert_string_not_empty(container_name, "container_name")
    assert_string_not_empty(file_prefix, "file_prefix")
    assert_string_not_empty(filename, "filename")
    assert_string_not_empty(creds_dir, "creds_dir")
    assert isinstance(
        write_to_single_file, bool
    ), f"Invalid input for 's3_dont_split_rows'; expected: str; received: {type(write_to_single_file)}"
    assert isinstance(
        dont_split_rows, bool
    ), f"Invalid input for 'write_to_single_file'; expected: str; received: {type(dont_split_rows)}"
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
                container_name=container_name,
                file_prefix=file_prefix,
                filename=filename,
                write_to_single_file=write_to_single_file,
                dont_split_rows=dont_split_rows,
                creds_dir=creds_dir,
                # When putting SQL into a TPT script, single quotes must be replaced with 2-single quotes
                sql=sql.replace("\n", " \n").replace("'", "''"),
            )
            return rendered_contents

    except Exception as e:
        logging.error(
            "Failed to render the `job_template.jinja` file from the provided args."
        )
        raise e


###################
#   AZURE CREDS   #
###################
def fill_azure_creds_template(
    storage_account_name: str,
    storage_account_key: str,
    azure_creds_template_path: str,
) -> str:
    """
    Fills the `credentials.jinja` (Azure) template with the values it needs.

    Args:
        storage_account_name:
        storage_account_key: SAS token
        azure_creds_template_path:

    Returns:
        Returns the azure_job.tpt file as a string (not yet written to disk)
    """
    logging.debug("Filling Azure template")

    # validate args
    assert_string_not_empty(storage_account_name, "storage_account_name")
    assert_string_not_empty(storage_account_key, "storage_account_key")
    assert os.path.exists(
        azure_creds_template_path
    ), f"Unable to find 'az_creds_template' at: {os.path.realpath(azure_creds_template_path)}"

    try:
        # Read and fill in template values
        with open(azure_creds_template_path, "r") as az_cred_template_file:
            rendered_contents = Template(az_cred_template_file.read()).render(
                storage_account_name=storage_account_name,
                storage_account_key=storage_account_key,
            )
            return rendered_contents

    except Exception as e:
        logging.error(
            "Failed to render the `credentials.jinja` file from the provided args."
        )
        raise e


def compile_job() -> None:
    """Generates the `job.tpt` & `teradata_creds.tpt` files passed in from the environment."""
    logging.info("Compiling Job")
    cloud_storage = os.getenv("CLOUD_STORAGE", "").upper()
    assert_string_not_empty(cloud_storage, "cloud_storage")

    # NOTE: The teradata_creds.tpt file is the same for all cloud storage
    with open("teradata_creds.tpt", "w") as tc_file:
        # Create `teradata_creds.tpt` file
        creds_text = fill_teradata_creds_template(
            teradata_database=os.getenv("TERADATA_DATABASE", ""),
            teradata_user=os.getenv("TERADATA_USER", ""),
            teradata_password=os.getenv("TERADATA_PASSWORD", ""),
            field_delimiter=os.getenv("FIELD_DELIMITER", "|"),
            teradata_creds_template_path="templates/teradata_creds_template.jinja",
        )
        # Write `creds_text` to `teradata_creds.tpt` (var_file)
        tc_file.write(creds_text)

    if cloud_storage == "S3":
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
                job_template_path="templates/s3/job_template.jinja",
            )
            # Write `job_text` to `job.tpt` (job_file)
            job_file.write(job_text)

    elif cloud_storage == "AZURE":
        # Create the (Azure) `job.tpt` file
        with open("job.tpt", "w") as job_file:
            # Create `job.tpt` file
            job_text = fill_azure_job_template(
                job_name=os.getenv("JOB_NAME", ""),
                container_name=os.getenv("CONTAINER_NAME", ""),
                file_prefix=os.getenv("FILE_PREFIX", ""),
                filename=os.getenv("FILENAME", ""),
                write_to_single_file=str_to_bool(
                    os.getenv("WRITE_TO_SINGLE_FILE", "False")
                ),
                dont_split_rows=str_to_bool(os.getenv("DONT_SPLIT_ROWS", "True")),
                creds_dir=os.getenv("CREDS_DIR", ""),
                sql=os.getenv("SQL", ""),
                job_template_path="templates/azure/job_template.jinja",
            )
            # Write `job_text` to `job.tpt` (job_file)
            job_file.write(job_text)

        # Create the `credentials` file
        with open("credentials", "w") as creds_file:
            creds_text = fill_azure_creds_template(
                storage_account_name=os.getenv("STORAGE_ACCOUNT_NAME", ""),
                storage_account_key=os.getenv("STORAGE_ACCOUNT_KEY", ""),
                azure_creds_template_path="templates/azure/credentials.jinja",
            )
            creds_file.write(creds_text)

    elif cloud_storage == "GCP":
        # TODO: Implement as necessary
        raise NotImplementedError("GCP storage has not yet been set up as a TARGET.")

    elif cloud_storage == "KAFKA":
        # TODO: Implement as necessary
        raise NotImplementedError("Kafka has not yet been set up as a TARGET.")

    else:
        raise NotImplementedError(f"{cloud_storage} has not yet been set up as a TARGET.")


if __name__ == "__main__":
    import subprocess

    # Compile job
    compile_job()

    # Run job
    subprocess.run("tbuild -f job.tpt -v teradata_creds.tpt".split())
