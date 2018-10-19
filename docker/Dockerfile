FROM hysds/pge-isce_giant

MAINTAINER mkarim "Mohammed.R.Karim@jpl.nasa.gov"
LABEL description="standard_product"

ARG git_oauth_token
ARG id
ARG gid

# provision
USER root
#RUN set -ex && \
 # usermod -u ${id} ops && \
  #groupmod -g ${gid} ops && \
  #chown -R ops:ops /home/ops/ /data && \
  #usermod -g ${gid} ops

# copy config
#COPY .aws /home/ops/.aws
#COPY .netrc /home/ops/.netrc

# copy packages
COPY . /home/ops/verdi/ops/standard_product

RUN pip install shapely
RUN pip3 install shapely

RUN pip install fiona
RUN pip3 install fiona
RUN sudo pip3 install geopandas

# as ops user
USER ops

# create work directory
RUN set -ex \
 && mkdir -p /data/work \
 && chmod -R 755 /data \
 && chown -R ops:ops /data


RUN set -ex \
 && sudo chown -R ops:ops /home/ops/verdi/ops/standard_product \
 && mv /home/ops/verdi/ops/standard_product/spyddder-man /home/ops/verdi/ops/ \
 && mv /home/ops/verdi/ops/standard_product/ariamh/ /home/ops/



WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
