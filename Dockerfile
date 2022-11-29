FROM node:lts

WORKDIR /usr/app/lessions

COPY package*.json ./

RUN npm install

COPY ./ ./

CMD ["npm" , "start"]