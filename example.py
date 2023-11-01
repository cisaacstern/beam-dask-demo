import glob
import json
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.dask.dask_runner import DaskRunner


def yield_jsonlines(fname: str):
    with open(fname) as f:
        for line in f.readlines():
            yield json.loads(line)
        

if __name__ == "__main__":
    opts = dict(runner=DaskRunner(), options=PipelineOptions(sys.argv[1:]))
    with beam.Pipeline(**opts) as p:
        (
            p
            | beam.Create(glob.glob('data/*.json'))
            | beam.FlatMap(yield_jsonlines)
            | beam.Filter(lambda record: record['age'] > 30)
            | beam.Filter(lambda record: record['name'][0].startswith('A'))
            | beam.Filter(lambda record: record['name'][1].startswith('B'))
            | beam.Filter(lambda record: record['occupation'].startswith('C'))
            | beam.Map(lambda record: (" ".join(record['name']), record['age'], record['occupation']))
            | beam.Map(print)
        )
