const fs = require('fs')
const _ = require('lodash')
const config = require('config')
const moment = require('moment')
const AWS = require('aws-sdk')
const AmazonS3URI = require('amazon-s3-uri')
const s3ToURL = require('s3-public-url')
const parse = require('csv-parse')
const rp = require('request-promise-native')
const m2mAuth = require('tc-core-library-js').auth.m2m
const m2m = m2mAuth(_.pick(config, ['AUTH0_URL', 'AUTH0_AUDIENCE', 'TOKEN_CACHE_TIME']))

const s3 = new AWS.S3()
AWS.config.region = config.get('aws.REGION')


// Database instance mapping
const dbs = {}
  // Database Document client mapping
const dbClients = {}

function getM2Mtoken() {
  console.log('*** getting M2M token')
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
    TableName: "Submission",
    IndexName: "legacySubmissionId-index",
    ProjectionExpression: "#url, #id, #legacySubmissionId",
    KeyConditionExpression: "#legacySubmissionId = :val0",
    ExpressionAttributeNames: {
      "#id": "id",
      "#url": "url",
      "#legacySubmissionId": "legacySubmissionId"
    },
    ExpressionAttributeValues: {
      ":val0": parseInt(legacySubmissionId)
    }
  }
  console.log(params)
  const dbClient = getDbClient()
  return await dbClient.query(params).promise()
}

async function moveFile(sourceBucket, sourceKey, targetBucket, targetKey) {
  console.log(`*** moving file ${sourceBucket}/${sourceKey} to ${targetBucket}/${targetKey}`)
  await s3.copyObject({
    Bucket: targetBucket,
    CopySource: `/${sourceBucket}/${sourceKey}`,
    Key: targetKey
  }).promise().then((data) => {
    console.log('moveop:', data)
  })
  console.log(`*** deleting file ${sourceBucket}/${sourceKey}`)
  return await s3.deleteObject({
    Bucket: sourceBucket,
    Key: sourceKey
  }).promise()
}

async function postEvent(payload) {
  const token = await getM2Mtoken()
  const opts = {
    headers: {
      'Authorization': `Bearer ${token}`
    },
    uri: config.BUS_URL,
    method: 'POST',
    json: true,
    resolveWithFullResponse: true,
    body: payload
  }

  console.log('sending event to bus ')
  return await rp(opts).then((data) => {
    console.log('*** posted event')
  })
}

async function updateSubURL(id, url) {
  const token = await getM2Mtoken()
  const opts = {
    headers: {
      'Authorization': `Bearer ${token}`
    },
    uri: `${config.SUBS_URL}/${id}`,
    method: 'PATCH',
    json: true,
    body: {
      url
    }
  }

  console.log(`*** PATCHING the sub with: ${url}`)
  return await rp(opts).then((data) => {
    console.log('*** patched the sub')
  })
}

async function loadRecords(csvFile) {
  fs.readFile(csvFile, (err, data) => {
    console.log('parsing CSV')
    parse(data, {
      comment: '#',
      delimiter: '\t'
    }, function(err, output) {
      _.forEach(output, async(row) => {
        let legacySubmissionId = row[0]
        let url = row[6]
        console.log(`${legacySubmissionId} | ${url}`)

        try {
          let r = await getRecord(legacySubmissionId)

          if (r.Items.length > 0) {
            let fileURL = r.Items[0].url
            let subId = r.Items[0].id
              //let file = await downloadFile(r.Items[0].url)
            if (/.*amazonaws.*/.test(fileURL)) {
              const {
                bucket, key
              } = AmazonS3URI(fileURL)
              let newURL = s3ToURL.getHttps(bucket, `${subId}.zip`)

              console.log(`${legacySubmissionId} | moving file on S3`)
              await moveFile(bucket, key, bucket, `${subId}.zip`)
              console.log(`${legacySubmissionId} | updating dynamo w/ new s3 URL`)
              let subResult = await updateSubURL(subId, newURL)
              console.log(subResult)
              console.log(`${legacySubmissionId} | posting a new unscanned event for file`)
              let eventResult = await postEvent({
                "topic": "avscan.action.scan",
                "originator": "lazybaer-subfixer",
                "timestamp": moment(new Date(), moment.ISO_8601),
                "mime-type": "application/json",
                "payload": {
                  "status": "unscanned",
                  "submissionId": subId,
                  "url": newURL,
                  "fileName": `${subId}.zip`
                }
              })
              console.log(eventResult)
            } else {
              console.log(`${legacySubmissionId} | file isn't on s3: ${fileURL}`)
            }
          } else {
            console.log(`${legacySubmissionId} | could not find records for legacySubmissionId`)
          }

        } catch (e) {
          console.log(e)
        }

        console.log(`${legacySubmissionId} | finished processing line`)
        console.log('------------------------------------------')
      });
    })
  })

}


return loadRecords('test.csv')
