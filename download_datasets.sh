mkdir datasets
cd datasets
wget -nc https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-03-20/comm_use_subset.tar.gz
wget -nc https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-03-20/noncomm_use_subset.tar.gz
wget -nc https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-03-20/custom_license.tar.gz
wget -nc https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-03-20/biorxiv_medrxiv.tar.gz
ls *.tar.gz | xargs -n1 tar -xzf