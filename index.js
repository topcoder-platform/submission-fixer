const fs = require('fs')
const _ = require('lodash')
const config = require('config')
  // const bluebird = require('bluebird')
const AWS = require('aws-sdk')
const AmazonS3URI = require('amazon-s3-uri')
const parse = require('csv-parse')


const s3 = new AWS.S3()
  // const s3p = bluebird.promisifyAll(s3)
AWS.config.region = config.get('aws.REGION')

// const s3 = bluebird.promisifyAll(s3)
const m2mAuth = require('tc-core-library-js').auth.m2m
const m2m = m2mAuth(_.pick(config, ['AUTH0_URL', 'AUTH0_AUDIENCE', 'TOKEN_CACHE_TIME']))

// Database instance mapping
const dbs = {}
  // Database Document client mapping
const dbClients = {}

function getM2Mtoken() {
  return m2m.getMachineToken(config.AUTH0_CLIENT_ID, config.AUTH0_CLIENT_SECRET)
}

function getDb() {
  if (!dbs['conn']) {
    dbs['conn'] = new AWS.DynamoDB()
  }
  return dbs['conn']
}

function getDbClient() {
  if (!dbClients['client']) {
    dbClients['client'] = new AWS.DynamoDB.DocumentClient()
  }
  return dbClients['client']
}

async function getRecord(legacySubmissionId) {
  const params = {
    TableName: 'Submission',
    FilterExpression: "legacySubmissionId = :val0",

    ExpressionAttributeValues: {
      ":val0": parseInt(legacySubmissionId)

    }
  };
  console.log(params)
  const dbClient = getDbClient()
  return new Promise((resolve, reject) => {
    dbClient.scan(params, (err, data) => {
      //  console.log('came back from dynamo with: ', err, data)
      if (err) {
        reject(err)
      }
      resolve(data)
    })
  })
}

async function downloadFile(fileURL) {
  let downloadedFile
  if (/.*amazonaws.*/.test(fileURL)) {
    const {
      bucket, key
    } = AmazonS3URI(fileURL)
    console.log(`downloadFile(): file is on S3 ${bucket} / ${key}`)
    downloadedFile = await s3.getObject({
      Bucket: bucket,
      Key: key
    })
    console.log('downloadedFile: ', downloadedFile)
    return downloadedFile.Body
  } else {
    throw Exception('not an S3 URL')
  }
}

async function loadRecords(csvFile) {
  fs.readFile(csvFile, (err, data) => {
    parse(data, {
      comment: '#',
      delimiter: '\t'
    }, function(err, output) {
      _.forEach(output, async(row) => {
        let legacySubmissionId = row[0]
        let url = row[6]
        console.log(legacySubmissionId, url)

        try {
          let r = await getRecord(legacySubmissionId)
          console.log(r)

          if (r.Items.length > 0) {
            let file = await downloadFile(r.Items[0].url)
            console.log(file)
          } else {
            console.log(`could not find recods for legacySubmissionId ${legacySubmissionId}`)
          }

        } catch (e) {
          console.log(e)
        }

        console.log('---')
      });
    })
  })

}


loadRecords('test.csv')
