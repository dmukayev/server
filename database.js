const{Pool}=require("pg")

const pool=new Pool({
    user: "postgres",
    password: "Sputnik111",
    host: "localhost",
    port: 5433
})


pool.query()