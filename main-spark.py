import subprocess


scripts=["REDDIT-Nettoyage-Posts.py","REDDIT-Nettoyage-Comms.py","STACK-Nettoyage-Posts.py","STACK-Nettoyage-Comms.py",
         "REDDIT-jointure.py","STACK-jointure.py","PLATFORM-Merge.py"]

for s in scripts:
    cmd = ["spark-submit", f"/app/scripts/{s}"]
    print(f"Exécution de {s}...")
    subprocess.run(cmd,check=True)

