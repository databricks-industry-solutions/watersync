import type React from 'react';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Switch,
} from '@databricks/appkit-ui/react';
import type { SourceColumn, JobSchedule } from '../types';

export function PageTitle({ title, description, action }: { title: string; description: string; action?: React.ReactNode }) {
  return (
    <div className="page-title mb-6 flex flex-wrap items-start justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
        <p className="mt-1 text-sm text-muted-foreground">{description}</p>
      </div>
      {action}
    </div>
  );
}

export function ErrorState({ message }: { message: string }) {
  return (
    <Alert variant="destructive">
      <AlertTitle>Data could not be loaded</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  );
}

export function Metric({
  icon: Icon,
  title,
  value,
  note,
  destructive = false,
  tone,
}: {
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  value: string | number;
  note: string;
  destructive?: boolean;
  tone?: 'primary' | 'info' | 'success';
}) {
  return (
    <Card
      className={`metric-card ${
        destructive
          ? 'metric-card-danger'
          : tone === 'primary'
            ? 'metric-card-primary'
            : tone === 'info'
              ? 'metric-card-info'
              : tone === 'success'
                ? 'metric-card-success'
                : ''
      }`}
    >
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardDescription className="font-medium uppercase tracking-wide">{title}</CardDescription>
          <div className="metric-icon">
            <Icon className="h-4 w-4" />
          </div>
        </div>
        <CardTitle className={`metric-value ${destructive ? 'text-destructive' : ''}`}>{value}</CardTitle>
      </CardHeader>
      <CardContent className="text-xs text-muted-foreground">{note}</CardContent>
    </Card>
  );
}

export function Hint({
  number,
  icon: Icon,
  title,
  text,
}: {
  number: string;
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  text: string;
}) {
  return (
    <div className="workflow-step">
      <div className="workflow-number">{number}</div>
      <div className="workflow-icon">
        <Icon className="h-5 w-5" />
      </div>
      <div>
        <div className="font-medium">{title}</div>
        <div className="mt-1 text-sm text-muted-foreground">{text}</div>
      </div>
    </div>
  );
}

export function ControlledField({
  id,
  label,
  value,
  onChange,
  required = false,
  type = 'text',
  disabled = false,
  hint,
}: {
  id: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  required?: boolean;
  type?: string;
  disabled?: boolean;
  hint?: string;
}) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        type={type}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        required={required}
        disabled={disabled}
      />
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}

export function ColumnSelect({
  id,
  label,
  value,
  columns,
  onChange,
  disabled = false,
  allowCombined = false,
}: {
  id: string;
  label: string;
  value: string;
  columns: SourceColumn[];
  onChange: (value: string) => void;
  disabled?: boolean;
  allowCombined?: boolean;
}) {
  const hasCombined = allowCombined && value.includes(',');
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{label}</Label>
      <Select value={value} onValueChange={onChange} disabled={disabled}>
        <SelectTrigger id={id}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="none">None</SelectItem>
          {hasCombined && <SelectItem value={value}>{value} (composite primary key)</SelectItem>}
          {columns.map((column) => (
            <SelectItem key={column.column_name} value={column.column_name}>
              {column.column_name} · {column.data_type}
              {column.is_primary_key === '1' ? ' · primary key' : ''}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

export function Field({
  name,
  label,
  value,
  required = false,
  disabled = false,
  hint,
}: {
  name: string;
  label: string;
  value?: string | null;
  required?: boolean;
  disabled?: boolean;
  hint?: string;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={name}>{label}</Label>
      <Input
        id={name}
        name={name}
        defaultValue={value ?? ''}
        required={required || name === 'group' || name === 'source'}
        disabled={disabled}
      />
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}

export function Toggle({
  name,
  label,
  checked,
  disabled = false,
  onCheckedChange,
}: {
  name: string;
  label: string;
  checked: boolean;
  disabled?: boolean;
  onCheckedChange?: (checked: boolean) => void;
}) {
  return (
    <label className={`flex items-center gap-3 rounded-md border p-3 text-sm ${disabled ? 'opacity-50' : ''}`}>
      <Switch
        name={name}
        checked={onCheckedChange ? checked : undefined}
        defaultChecked={onCheckedChange ? undefined : checked}
        onCheckedChange={onCheckedChange}
        disabled={disabled}
      />
      {label}
    </label>
  );
}

export function ScheduleFields({
  enabled,
  onEnabledChange,
  active,
  onActiveChange,
  schedule,
}: {
  enabled: boolean;
  onEnabledChange: (value: boolean) => void;
  active: boolean;
  onActiveChange: (value: boolean) => void;
  schedule?: JobSchedule;
}) {
  return (
    <div className="space-y-4">
      <label className="flex items-center justify-between gap-4 rounded-md border p-3 text-sm">
        <span>
          <span className="block font-medium">Scheduled execution</span>
          <span className="text-xs text-muted-foreground">Run automatically using a Quartz cron schedule.</span>
        </span>
        <Switch checked={enabled} onCheckedChange={onEnabledChange} aria-label="Scheduled execution" />
      </label>
      <div className="grid gap-4 md:grid-cols-2">
        <div className="space-y-2 md:col-span-2">
          <Label htmlFor="quartz-cron">Quartz cron expression</Label>
          <Input
            id="quartz-cron"
            name="quartzCronExpression"
            defaultValue={schedule?.quartz_cron_expression ?? '0 0 8 * * ?'}
            placeholder="0 0 8 * * ?"
            required
            disabled={!enabled}
          />
          <p className="text-xs text-muted-foreground">Seconds, minutes, hours, day of month, month, day of week.</p>
        </div>
        <div className="space-y-2">
          <Label htmlFor="schedule-timezone">Timezone</Label>
          <Input
            id="schedule-timezone"
            name="timezoneId"
            defaultValue={schedule?.timezone_id ?? 'America/New_York'}
            placeholder="America/New_York"
            required
            disabled={!enabled}
          />
        </div>
        <label className="flex items-center justify-between gap-4 rounded-md border p-3 text-sm">
          <span>
            <span className="block font-medium">Schedule active</span>
            <span className="text-xs text-muted-foreground">Turn off to save the schedule in a paused state.</span>
          </span>
          <Switch checked={active} onCheckedChange={onActiveChange} aria-label="Schedule active" disabled={!enabled} />
        </label>
      </div>
    </div>
  );
}
