module.exports = {
  aws: {
    REGION: 'us-east-1'
  },
  LOG_LEVEL: 'debug',
  AUTH0_URL: process.env.AUTH0_URL || '',
  AUTH0_AUDIENCE: process.env.AUTH0_AUDIENCE || '',
  TOKEN_CACHE_TIME: process.env.TOKEN_CACHE_TIME || '',
  AUTH0_CLIENT_ID: process.env.AUTH0_CLIENT_ID || '',
  AUTH0_CLIENT_SECRET: process.env.AUTH0_CLIENT_SECRET || '',
  BUS_URL: process.env.BUS_URL || '',
  SUBS_URL: process.env.SUBS_URL || ''
}
