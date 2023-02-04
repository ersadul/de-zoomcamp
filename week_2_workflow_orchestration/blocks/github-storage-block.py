from prefect.filesystems import GitHub

g = GitHub(repository="https://github.com/ersadul/de-zoomcamp.git")
g.save("ersadul-github-block", overwrite=True)
print('block created')