# DevContainer image
FROM debian:bookworm-slim
RUN \
    adduser --system --disabled-password --shell /bin/bash --home /home/vscode vscode && \
    # docker setup
    apt-get update && \
    apt-get install ca-certificates curl gnupg lsb-release -y && \
    mkdir -m 0755 -p /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y && \
    usermod -aG docker vscode && \
    apt-get clean
RUN \
    # common dev setup
    apt update && \
    apt-get install sudo curl jq git bash-completion -y && \
    usermod -aG sudo vscode && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    echo '. /etc/bash_completion' >> /home/vscode/.bashrc && \
    echo 'export PS1="\[\e[32;1m\]\u\[\e[m\]@\[\e[34;1m\]\H\[\e[m\]:\[\e[33;1m\]\w\[\e[m\]$ "' >> /home/vscode/.bashrc && \
    chown vscode:nogroup /home/vscode/.bashrc && \
    apt-get clean
    
USER vscode
ENV PATH=/home/vscode/.local/bin:$PATH
WORKDIR /home/vscode
COPY --chown=vscode:nogroup src/orcapod/requirements.txt /tmp/requirements.txt
RUN \
    # python setup
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    uv venv -p 3.9 ~/.local/share/base && \
    uv pip install --no-cache-dir --upgrade pip flake8 black faker ipykernel -p ~/.local/share/base && \
    uv pip install --no-cache-dir -r /tmp/requirements.txt -p ~/.local/share/base && \
    rm /tmp/requirements.txt && \
    echo '. ~/.local/share/base/bin/activate' >> ~/.bashrc
ENV VIRTUAL_ENV=/home/vscode/.local/share/base

CMD ["bash", "-c", "sudo rm /var/run/docker.pid; sudo dockerd"]