from prefect.filesystems import GitHub

g = GitHub(repository="https://github.com/ersadul/de-zoomcamp.git")
g.save("ersadul-github-block", overwrite=True)
#g.get_directory(from_path="week_2_workflow_orchestration/flows/02_gcp/", local_path="./blocks/")
print('block created')


# prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "github-sc-flow" \
#     --storage-block="github/ersadul-github-block" \
#     --path="week_2_workflow_orchestration/blocks/week_2_workflow_orchestration/flows/02_gcp/" \
#     --params='{"color": "green", "months": [11], "year": "2020"}' \
#     --apply

# prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs --name github-sc-flow -sb github/ersadul-github-block --path="week_2_workflow_orchestration/blocks/week_2_workflow_orchestration/flows/02_gcp/"