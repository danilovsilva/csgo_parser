import ftplib
import os
import re
from passwd import userftp,passwdftp

# Conectando ao servidor FTP
ftp = ftplib.FTP('ftp.maggie.hostzone.games')
ftp.login(userftp, passwdftp)

# Navegando para o diretório onde estão os arquivos .dem
ftp.cwd('/csgo/')

# Listando os arquivos no diretório
arquivos = ftp.nlst()

# Escolhendo a pasta onde os arquivos serão salvos
pasta_destino = 'C:\projects\csgo_parser\demos'

# Criando a pasta de destino, se ela não existir
if not os.path.exists(pasta_destino):
    os.makedirs(pasta_destino)

# Baixando os arquivos .dem para a pasta de destino
for arquivo in arquivos:
    if arquivo.endswith('.dem'):
        caminho_arquivo = os.path.join(pasta_destino, arquivo)
        if not os.path.exists(caminho_arquivo):
            with open(caminho_arquivo, 'wb') as f:
                ftp.retrbinary('RETR ' + arquivo, f.write)

# Fechando a conexão com o servidor FTP
ftp.quit()