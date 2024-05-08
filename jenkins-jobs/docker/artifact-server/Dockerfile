FROM svenstaro/miniserve:alpine

RUN apk add --no-cache bash

ADD ./plugins /opt/plugins
ADD listing.sh /opt/

RUN chmod +x /opt/listing.sh
RUN /opt/listing.sh -d /opt/plugins -o /opt/plugins/artifacts.txt

ENTRYPOINT ["/app/miniserve"]
CMD ["/opt/plugins"]
