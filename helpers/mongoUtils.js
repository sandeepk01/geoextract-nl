const GeoJSON = require('mongoose-geojson-schema');
const mongoose = require('mongoose');
const DB_CONN = 'mongodb://localhost:27017/?readPreference=primary&ssl=false';

const AddressModel = mongoose.model(
    'ADDRESS',
    new mongoose.Schema(
        {
            _id: String,
            buildingNumber: String,
            street: String,
            postcode: String,
            town: String,
            latitude: Number,
            longitude: Number,
            municipality: String,
            huisNummer: String,
            huisLetter: String,
            toEvoeging: String,
            straatNaam: String,
            woonplaats: String,
            gemeente: String,
            geometry: mongoose.Schema.Types.Point,
        },
        { _id: false }
    )
);

function closeDB() {
    return new Promise((resolve, reject) => {
        mongoose.connection
            .close()
            .then(() => {
                console.log('MongoDB - Disconnected');
                resolve();
            })
            .catch((err) => {
                reject(err);
            });
    });
}

function openDB() {
    return new Promise((resolve, reject) => {
        mongoose
            .connect(DB_CONN, {
                useNewUrlParser: true,
                useUnifiedTopology: true,
                dbName: 'locationdata_nl',
                family: 4,
                serverSelectionTimeoutMS: 5000,
            })
            .then((conn) => {
                console.log('MongoDB - Connected');
                resolve(conn);
            })
            .catch((err) => {
                reject(err);
            });
    });
}

async function insertBulk(list) {
    await AddressModel.insertMany(list);
}

module.exports = { openDB, insertBulk, closeDB };
