use crux_core::{App, Command, Effect};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Placement {
    Local,
    Reverse,
    Box,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SurfaceEvent {
    pub placement: Placement,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SurfaceModel {
    pub placement: Option<Placement>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SurfaceViewModel {
    pub placement: Option<Placement>,
}

pub enum SurfaceEffect {}

impl Effect for SurfaceEffect {}

#[derive(Default)]
pub struct Surface;

impl App for Surface {
    type Event = SurfaceEvent;
    type Model = SurfaceModel;
    type ViewModel = SurfaceViewModel;
    type Effect = SurfaceEffect;

    fn update(
        &self,
        event: SurfaceEvent,
        model: &mut SurfaceModel,
    ) -> Command<SurfaceEffect, SurfaceEvent> {
        model.placement = Some(event.placement);
        Command::done()
    }

    fn view(&self, model: &SurfaceModel) -> SurfaceViewModel {
        SurfaceViewModel {
            placement: model.placement,
        }
    }
}

#[cfg(test)]
mod tests {
    use crux_core::App;

    use super::{Placement, Surface, SurfaceEvent, SurfaceModel, SurfaceViewModel};

    #[test]
    fn one_local_event_selects_a_placement() {
        let app = Surface;
        for placement in [Placement::Local, Placement::Reverse, Placement::Box] {
            let mut model = SurfaceModel { placement: None };
            let mut command = app.update(SurfaceEvent { placement }, &mut model);
            assert!(command.is_done());
            assert_eq!(model.placement, Some(placement));
            let first = app.view(&model);
            let second = app.view(&model);
            assert_eq!(
                first,
                SurfaceViewModel {
                    placement: Some(placement),
                }
            );
            assert_eq!(first, second);
        }
    }
}
