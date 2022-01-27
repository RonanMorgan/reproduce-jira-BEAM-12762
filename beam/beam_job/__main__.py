import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.textio import WriteToText

def main():

    sdk_harness_endpoint = os.getenv('SDK_HARNESS_ENDPOINT', 'worker-pool:50000')
    job_service_endpoint = os.getenv('JOB_SERVICE_ENDPOINT', 'job-service:8099')

    options = PipelineOptions([
                      "--runner=PortableRunner",
                      "--environment_config="+sdk_harness_endpoint,
                      "--environment_type=EXTERNAL",
                      "--job_endpoint=" + job_service_endpoint,
                      "--artifact_endpoint=job-service:8098"
                  ])

    with beam.Pipeline(options=options) as p:

        lines = ( p | beam.Create(['Hello World.', 'Apache beam']) )

        # Write to local filesystem
        ( lines | WriteToText('/tmp/sample.txt') )

if __name__ == "__main__":

    main()
