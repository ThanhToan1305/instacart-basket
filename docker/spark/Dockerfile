FROM apache/spark:3.5.7-java17-python3

USER root
COPY requirements-ml.txt /opt/spark/requirements-ml.txt
RUN python3 -m pip install --no-cache-dir --only-binary=:all: -r /opt/spark/requirements-ml.txt
# Import the bundled PySpark (do not install a second Spark version through pip).
RUN python3 -c "import glob,sys; sys.path[:0]=['/opt/spark/python']+glob.glob('/opt/spark/python/lib/*.zip'); import numpy; from pyspark.ml.fpm import FPGrowth, FPGrowthModel; print('ML_IMPORT_PASS numpy='+numpy.__version__)"
USER spark
