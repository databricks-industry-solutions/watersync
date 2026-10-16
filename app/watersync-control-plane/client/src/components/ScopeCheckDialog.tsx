import { useEffect, useState } from 'react';
import {
  Badge,
  Button,
  Checkbox,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@databricks/appkit-ui/react';
import { api } from '../lib/api';
import { reportWarning } from '../lib/logging';

type ScopeStatus = {
  mode: 'user' | 'development' | 'missing-token';
  scopes: Array<{ scope: string; usedFor: string; impactIfMissing: string; granted: boolean | null }>;
};

const dismissedKey = 'watersync.scope-check.dismissed';

/**
 * Checks at startup that the signed-in user's token carries every user authorization scope the app
 * needs, and explains what is missing and how to fix it.
 */
export function ScopeCheckDialog() {
  const [status, setStatus] = useState<ScopeStatus | null>(null);
  const [open, setOpen] = useState(false);

  const missing = status?.scopes.filter((item) => item.granted === false).map((item) => item.scope) ?? [];
  const missingKey = missing.join(',');

  useEffect(() => {
    api<ScopeStatus>('/api/auth/scopes')
      .then((result) => {
        setStatus(result);
        const missingNow = result.scopes.filter((item) => item.granted === false).map((item) => item.scope);
        if (missingNow.length && sessionStorage.getItem(dismissedKey) !== missingNow.join(',')) setOpen(true);
      })
      .catch((error) => {
        reportWarning('auth.scope_check_failed', 'Could not check user authorization scopes', {
          detail: String(error).slice(0, 200),
        });
      });
  }, []);

  if (!status) return null;

  const dismiss = () => {
    sessionStorage.setItem(dismissedKey, missingKey);
    setOpen(false);
  };

  return (
    <Dialog open={open} onOpenChange={(value) => (value ? setOpen(true) : dismiss())}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>
            {status.mode === 'missing-token'
              ? 'User authorization is not enabled'
              : 'Missing user authorization scopes'}
          </DialogTitle>
          <DialogDescription>
            WaterSync runs every action with your own Databricks credentials. Your access token is missing{' '}
            {status.mode === 'missing-token' ? 'entirely' : `${missing.length} of the scopes the app needs`}, so some
            features will fail.
          </DialogDescription>
        </DialogHeader>
        <ul className="grid max-h-[50vh] gap-3 overflow-y-auto py-2">
          {status.scopes.map((item) => (
            <li key={item.scope} className="flex gap-3 rounded-md border p-3">
              <Checkbox
                id={`scope-${item.scope}`}
                checked={item.granted === true}
                disabled
                aria-label={`${item.scope} ${item.granted ? 'enabled' : 'not enabled'}`}
                className="mt-0.5"
              />
              <div className="grid gap-1 text-sm">
                <label htmlFor={`scope-${item.scope}`} className="flex items-center gap-2 font-medium">
                  <code>{item.scope}</code>
                  <Badge variant={item.granted ? 'secondary' : 'destructive'}>
                    {item.granted ? 'Enabled' : 'Not enabled'}
                  </Badge>
                </label>
                <span className="text-muted-foreground">{item.usedFor}</span>
                {!item.granted && <span className="text-destructive">Without it: {item.impactIfMissing}</span>}
              </div>
            </li>
          ))}
        </ul>
        <div className="grid gap-1 rounded-md bg-muted p-3 text-sm">
          <span className="font-medium">How to fix</span>
          <ol className="list-decimal space-y-1 pl-5 text-muted-foreground">
            <li>
              Add the missing scopes to the app: in Databricks open <b>Compute → Apps →</b> this app{' '}
              <b>→ Edit → User authorization</b>, or list them under <code>user_api_scopes</code> in{' '}
              <code>databricks.yml</code> and redeploy.
            </li>
            <li>If a scope cannot be selected, ask a workspace admin to allow it for apps.</li>
            <li>
              Reload this page and accept the permission prompt. Existing sessions keep their old token until then.
            </li>
          </ol>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => window.location.reload()}>
            Reload
          </Button>
          <Button onClick={dismiss}>Continue anyway</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
