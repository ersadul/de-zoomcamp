from prefect.filesystems import GitHub

g = GitHub(repository="https://github.com/ersadul/de-zoomcamp.git")
g.save("ersadul-github-block", overwrite=True)
g.get_directory(from_path="week_2_workflow_orchestration/flows/02_gcp/", local_path="./blocks/")
print('block created and file pulled')
