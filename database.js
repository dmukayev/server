const{Pool}=require("pg")

const pool=new Pool({
    user: "postgres",
    password: "Sputnik11",
    host: "localhost",
    port: 5433
})


pool.query()
