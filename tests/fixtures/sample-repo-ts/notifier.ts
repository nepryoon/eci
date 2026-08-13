interface Notifier {
  notify(message: string): void;
}
class EmailNotifier implements Notifier {
  notify(message: string): void { console.log(message); }
}
