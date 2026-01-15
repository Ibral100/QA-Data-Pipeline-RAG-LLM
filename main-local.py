import subprocess


print("Lancement des scripts sur docker")
subprocess.run([
    "docker", "exec", "spark-master-1",
    "spark-submit", "/app/scripts/main-spark.py"
], check=True)


print("\nLancement du script local")
subprocess.run(["python", "PFE/PLATFORM-contextual-Chunking.py"], check=True)
