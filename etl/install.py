import subprocess
import os
import sys
import urllib.request
import platform


is_windows = platform.system() == "Windows"
subprocess.run([sys.executable, "-m", "venv", "env"])

activate_script = ''
if is_windows:
    activate_script = os.path.join("env", "Scripts", "activate.bat")
else:
    activate_script = os.path.join("env", "bin", "activate")

activate_command = ''
if is_windows:
    activate_command = f"{activate_script} &&"
else:
    activate_command = f"source {activate_script} &&"

subprocess.run(activate_command + f" {sys.executable} -m pip install -r requirements.txt", shell=True)

jars_path = os.path.join("env", "lib", "python3.10", "site-packages", "pyspark", "jars")
os.makedirs(jars_path, exist_ok=True)

urls = [
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.3/hadoop-azure-3.3.3.jar",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.3/hadoop-azure-datalake-3.3.3.jar",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.3/hadoop-common-3.3.3.jar"
]

for url in urls:
    file_name = os.path.join(jars_path, os.path.basename(url))
    urllib.request.urlretrieve(url, file_name)

print('Necessário instalar o JDK 17. Caso já possua em sua máquina, execute o comando: export JAVA_HOME="<local de instalação do JDK>" dentro do ambiente virtual python.')
