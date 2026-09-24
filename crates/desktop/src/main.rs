use gpui_kit::base::StyledExt as _;
use gpui_kit::component::button::*;
use gpui_kit::component::Root;
use gpui_kit::*;

struct Desk;

impl Render for Desk {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div()
            .v_flex()
            .p_4()
            .gap_2()
            .child("gol")
            .child("Control decides what may run. Execution decides where. The harness decides how.")
            .child(Button::new("local").primary().label("Local"))
            .child(Button::new("reverse").label("Reverse"))
            .child(Button::new("box").label("Box"))
    }
}

fn main() {
    gpui_kit::application().run(|cx| {
        gpui_kit::init(cx);
        cx.spawn(async move |cx| {
            cx.open_window(WindowOptions::default(), |window, cx| {
                let view = cx.new(|_| Desk);
                cx.new(|cx| Root::new(view, window, cx))
            })
            .expect("open window");
        })
        .detach();
    });
}
