FROM coady/pylucene

USER root

WORKDIR /data

RUN pip install jupyter

EXPOSE 8890

CMD ["jupyter-notebook", "--allow-root", "--ip=0.0.0.0", "--port=8890"]