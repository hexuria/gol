#![forbid(unsafe_code)]
use crux_core::{App, Command, Effect};
use protocol::ExecutionPlacement;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SurfaceEvent {
    pub placement: ExecutionPlacement,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SurfaceModel {
    pub placement: Option<ExecutionPlacement>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SurfaceViewModel {
    pub placement: Option<ExecutionPlacement>,
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
    use protocol::ExecutionPlacement;

    use super::{Surface, SurfaceEvent, SurfaceModel, SurfaceViewModel};

    #[test]
    fn one_local_event_selects_a_placement() {
        let app = Surface;
        for placement in [
            ExecutionPlacement::Local,
            ExecutionPlacement::Reverse,
            ExecutionPlacement::Box,
        ] {
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
