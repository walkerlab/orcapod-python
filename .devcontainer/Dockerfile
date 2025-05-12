# DevContainer image
FROM debian:bookworm-slim
RUN \
    adduser --system --disabled-password --shell /bin/bash --home /home/vscode vscode
RUN \
    # common dev setup
    apt update && \
    apt-get install sudo curl jq git bash-completion graphviz libgraphviz-dev gcc g++ -y && \
    curl https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -Lo /usr/local/bin/yq && \
    chmod +x /usr/local/bin/yq && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y && \
    echo ". ~/.cargo/env" >> ~/.bashrc && \
    usermod -aG sudo vscode && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    echo '. /etc/bash_completion' >> /home/vscode/.bashrc && \
    echo 'export PS1="\[\e[32;1m\]\u\[\e[m\]@\[\e[34;1m\]\H\[\e[m\]:\[\e[33;1m\]\w\[\e[m\]$ "' >> /home/vscode/.bashrc && \
    chown vscode:nogroup /home/vscode/.bashrc && \
    apt-get clean
    
USER vscode
ENV PATH=/home/vscode/.local/bin:$PATH
WORKDIR /home/vscode
COPY --chown=vscode:nogroup requirements.txt /tmp/requirements.txt
RUN \
    # python setup
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    uv venv -p 3.9 ~/.local/share/base && \
    uv pip install --no-cache-dir --upgrade pip flake8 black faker ipykernel -p ~/.local/share/base && \
    uv pip install --no-cache-dir -r /tmp/requirements.txt -p ~/.local/share/base && \
    rm /tmp/requirements.txt && \
    echo '. ~/.local/share/base/bin/activate' >> ~/.bashrc
ENV VIRTUAL_ENV=/home/vscode/.local/share/base

CMD ["tail", "-f", "/dev/null"]