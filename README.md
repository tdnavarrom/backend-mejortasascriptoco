# Fast-API Backend MejorTasasCripto.co

## Scheduled fetcher with cron-job.org

The backend exposes a protected cron endpoint at `/api/cron/fetcher`.

### 1. Configure Vercel environment variables

Add this variable in your Vercel project settings:

```env
CRON_SECRET=replace-with-a-long-random-string
```

Leave `CRON_ALLOWED_IPS` unset unless you explicitly want IP allowlisting.

### 2. Configure the cron-job.org job

Create a job with these settings:

- URL: `https://your-domain/api/cron/fetcher`
- Method: `GET`
- Schedule: every 15 minutes
- Request header:

```text
Authorization: Bearer <your CRON_SECRET>
```

### 3. How the protection works

The route only runs the fetcher when the request includes:

```text
Authorization: Bearer <your CRON_SECRET>
```

If the header is missing or incorrect, the API returns `401 Unauthorized`.

### 4. Optional IP allowlisting

If you want stricter access control, you can also set:

```env
CRON_ALLOWED_IPS=116.203.134.67,116.203.129.16,23.88.105.37,128.140.8.200,91.99.23.109
```

Only use this if you are comfortable updating the list when cron-job.org changes executor nodes.

### 5. Important timeout caveat

cron-job.org free jobs have a short timeout window. If `run_fetcher()` takes too long, the cron request may fail even if the endpoint itself is configured correctly.
