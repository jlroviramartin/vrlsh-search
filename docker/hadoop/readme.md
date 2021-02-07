Configuración correcta para datanodes.

1. docker-compose.yml
```
datanode<i>:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode<i>
    hostname: datanode<i>.hadoop.es
```

2. hosts
```
127.0.0.1 datanode<i>.hadoop.es
```

3. nginx.conf
```
server {
    server_name datanode<i>.hadoop.es;
    listen 9864;
    location / {
        proxy_pass http://datanode<i>:9864;
    }
}
```

Para acceder a los datanodes: ```http://datanode<i>.hadoop.es:9864```

## Nginx

- [nginx.conf Full Example Configuration](https://www.nginx.com/resources/wiki/start/topics/examples/full/)
- [Virtual Hosts on nginx](# https://gist.github.com/soheilhy/8b94347ff8336d971ad0)
- [How to set up an easy and secure reverse proxy with Docker, Nginx & Letsencrypt](https://www.freecodecamp.org/news/docker-nginx-letsencrypt-easy-secure-reverse-proxy-40165ba3aee2/)
- [NGINX Solution for Apache ProxyPassReverse](https://www.nginx.com/resources/wiki/start/topics/examples/likeapache/)
- [Nginx: Everything about proxy_pass](https://dev.to/danielkun/nginx-everything-about-proxypass-2ona)
- [Nginx proxy_pass: examples for how does nginx proxy_pass map the request](https://www.liaohuqiu.net/posts/nginx-proxy-pass/)
- [How to Use NGINX as an HTTPS Forward Proxy Server](https://www.alibabacloud.com/blog/how-to-use-nginx-as-an-https-forward-proxy-server_595799)
- [Setting up a Reverse-Proxy with Nginx and docker-compose](https://www.domysee.com/blogposts/reverse-proxy-nginx-docker-compose)
- [How To Create Temporary and Permanent Redirects with Nginx](https://www.digitalocean.com/community/tutorials/how-to-create-temporary-and-permanent-redirects-with-nginx)
- [Setting up a Reverse-Proxy with Nginx and docker-compose](https://www.domysee.com/blogposts/reverse-proxy-nginx-docker-compose)
