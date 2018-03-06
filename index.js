const connection = require('tedious').Connection;
const request = require('tedious').Request;
const TYPES = require('tedious').TYPES;
const async = require('async');
const feathers = require('@feathersjs/feathers')
const rest = require('@feathersjs/rest-client')
const axios = require('axios')
const moment = require('moment')

const url = 'http://localhost:3030'
const config = {
    userName: 'sa',
    password: 'SAPB1Admin',
    server: 'WIN-TR0HMI56UON',
    options: {
        database: 'SBOPruebaMX'
    }
}

const conexion = new connection(config);
const productoService = getProductosService()

function getProductosService () {
    const app = feathers()
    const cliente = rest(url)

    app.configure(cliente.axios(axios))

    return app.service('productos')
}

function leerProductos (callback) {
    const fecha = moment().subtract(2, 'days').format('YYYYMMDD')
    const productos = [];
    const sql = `
        SELECT o.DocEntry, ISNULL(a.LogInstanc, 0) Instancia, o.ItemCode, o.ItemName
        FROM OITM o 
        LEFT JOIN AITM a ON o.DocEntry = a.DocEntry
        LEFT JOIN BXP_SYNCProductos bs ON o.DocEntry = bs.DocEntry
        WHERE
            (o.CreateDate >= '${fecha}' OR a.UpdateDate >= '${fecha}')
            AND ISNULL(a.LogInstanc, 0) > ISNULL(bs.logInstance, -1)
    `;

    console.log(sql)
    const query = new request(sql, function(err, rowCount) {
    });

    query.on('row', function(columns) {
        productos.push({
            docEntry: columns[0].value,
            instancia: columns[1].value,
            itemCode: columns[2].value,
            itemName: columns[3].value
        });
    });

    query.on('requestCompleted', function() {
        callback(null, productos);
    });

    conexion.execSql(query);
}

function buscarProducto (producto, callback) {
    productoService.find({query: { itemCode: producto.itemCode }})
        .then(function (response) {
            const id = response.total > 0 ? response.data[0]._id : null
            callback(null, {id: id, data: producto})
        })
        .catch(function (error) {
            callback(error)
        })
}

 function upsertProducto (producto, callback) {
    if (producto.id) {
        productoService.update(producto.id, producto.data)
            .then(function (response) {
                callback(null, producto.data)
            })
            .catch(function (error) {
                callback(error)
            })
    } else {
        productoService.create(producto.data)
            .then(function (response) {
                callback(null, producto.data)
            })
            .catch(function (error) {
                callback(error)
            })
    }
}

function marcarActualizado (producto, callback) {
    const sql = `
        DELETE FROM BXP_SYNCProductos WHERE ItemCode = '${producto.itemCode}';
        INSERT INTO BXP_SYNCProductos (DocEntry, ItemCode, ItemName, logInstance)
        VALUES (@docEntry, @itemCode, @itemName, @instancia);
    `;
    console.log(producto)
    console.log(sql)
    const query = new request(sql, function(err, rowCount, rows) {
        if (err) {
            console.log(err)
            callback(err)
        } else {
            callback(null)
        }
    });

    query.addParameter('docEntry', TYPES.Int, producto.docEntry)
    query.addParameter('itemCode', TYPES.VarChar, producto.itemCode)
    query.addParameter('itemName', TYPES.VarChar, producto.itemName)
    query.addParameter('instancia', TYPES.Int, producto.instancia)

    conexion.execSql(query);
}

function actualizarProductos (productos, mainCallback) {
    const syncProducto = async.compose(marcarActualizado, upsertProducto, buscarProducto)

    async.eachSeries(productos, function (producto, callback) {
        syncProducto(producto, function (error, result) {
            if (error) callback(error)
            else callback(null)
        })
    }, function (err) {
        if (err) {
            mainCallback(err)
        } else {
            mainCallback(null)
        }
    })

} // actualizarProductos

function Complete(err, result) {
    if (err) {
        console.log(err.message)
    } else {
        console.log('Terminado');
    }
    conexion.close()
}

conexion.on('connect', function(err) {
    if (err) {
        console.log(err);
    } else {
        console.log('conectado');

        async.waterfall([leerProductos, actualizarProductos], Complete)
    }
});

