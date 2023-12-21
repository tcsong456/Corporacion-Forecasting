FROM python:3.9-slim

WORKDIR corpor
COPY . .

RUN set -e \
  && apt-get update \
  && apt-get install --no-install-recommends -y unzip \
                                                p7zip-full \
                                                dos2unix \
                                                gawk \
#need to download kaggle.json first on your kaggle acount
  && mkdir /root/.kaggle \
  && cp kaggle.json /root/.kaggle/kaggle.json \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --no-cache-dir -r requirements.txt \ 
  && dos2unix shell_scripts/download.sh \
  && dos2unix shell_scripts/process_data.sh

ENTRYPOINT ["/bin/bash","-c","bash shell_scripts/download.sh && time bash shell_scripts/process_data.sh"]